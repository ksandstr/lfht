
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <sched.h>
#include <errno.h>

#include <ccan/likely/likely.h>
#include <ccan/container_of/container_of.h>

#include "lfht.h"
#include "epoch.h"


#define MIN_SIZE_LOG2 LFHT_MIN_TABLE_SIZE
#define MIN_PROBE (64 * 2 / sizeof(uintptr_t))

#define POPCOUNT(x) __builtin_popcountl((x))
#define MSB(x) (sizeof((x)) * 8 - __builtin_clzl((x)) - 1)

#define MY_PERCPU(t) ((struct lfht_table_percpu *)percpu_my((t)->pc))
#define ELEMS(t) (MY_PERCPU((t))->elems)
#define DELETED(t) (MY_PERCPU((t))->deleted)


#define increase_to(ptr, val) do { \
		typeof((val)) _v = (val); \
		_Atomic typeof(_v) *_p = (ptr); \
		typeof(_v) _o = *_p; \
		while(_o < _v && !atomic_compare_exchange_weak(_p, &_o, _v)) \
			; \
	} while(false)


static struct lfht_table *next_table_gen(
	const struct lfht *ht, const struct lfht_table *prev, bool filter_halted);


#ifndef NDEBUG
/* TODO: decode the migration pointer format as well */
static inline char *format_entry(
	char tmp[static 100], struct lfht_table *t, uintptr_t e)
{
	snprintf(tmp, 100, "%#lx [%c%c%c%c%c]", e,
		(e & t->src_bit) != 0 ? 's' : '-',
		(e & t->mig_bit) != 0 ? 'M' : '-',
		(e & t->hazard_bit) != 0 ? 'h' : '-',
		(e & t->ephem_bit) != 0 ? 'e' : '-',
		(e & t->del_bit) != 0 ? 'D' : '-');
	return tmp;
}
#endif


static int size_to_log2(size_t s) {
	int msb = MSB(s);
	return (1ul << msb) < s ? msb + 1 : msb;
}


/* true if @e (from @t->table) terminates probing. */
static inline bool is_void(const struct lfht_table *t, uintptr_t e) {
	return (e & ~t->mig_bit) == 0;
}


/* true if @e (from @t->table) is a valid slot for ht_add(). */
static inline bool is_empty(const struct lfht_table *t, uintptr_t e) {
	return (e & ~(t->del_bit | t->hazard_bit)) == 0;
}


/* true if @e (from @t->table) represents a value given in lfht_add(). */
static inline bool is_val(const struct lfht_table *t, uintptr_t e) {
	return !is_void(t, e) && (e & (t->del_bit | t->mig_bit)) == 0;
}


/* special values of the mig_bit format. */
static inline uintptr_t mig_void(const struct lfht_table *t) {
	assert(is_void(t, t->mig_bit));
	assert(!is_val(t, t->mig_bit));
	return t->mig_bit;
}


static inline uintptr_t mig_val(const struct lfht_table *t) {
	uintptr_t e = (t->mig_bit + 1) | t->mig_bit;
	assert(!is_void(t, e));
	assert(!is_val(t, e));
	return e;
}


/* decode the target gen_id for @e found in @t->table[]. */
static inline unsigned long mig_gen_id(
	const struct lfht_table *t, uintptr_t e)
{
	assert((e & t->mig_bit) != 0);
	e = (e & (t->mig_bit - 1)) | ((e >> 1) & ~(t->mig_bit - 1));
	return t->gen_id + (e & 0x3ff);
}


/* decode the slot index in @dst->table[] designated by migration pointer @e
 * in source table @src, for an entry that hashes to @hash.
 */
static inline size_t mig_slot(
	const struct lfht_table *src, size_t hash, uintptr_t e,
	const struct lfht_table *dst)
{
	/* unpack around @src->mig_bit, discard gen_id. */
	assert((e & src->mig_bit) != 0);
	e = (e & (src->mig_bit - 1)) | ((e >> 1) & ~(src->mig_bit - 1));
	uintptr_t addr = e >> 10;

	/* combine hash, overflow bit, and the position in @e. */
	uintptr_t dst_mask = (1ul << dst->size_log2) - 1,
		bit = 1ul << dst->probe_addr_size_log2,
		slot = (addr & ~bit) | ((hash + (addr & bit)) & ~(bit - 1) & dst_mask);
	assert((slot & ~dst_mask) == 0);	/* unused bits are 0 in `addr' */

	return slot;
}


/* compute the correct probe_addr parameter to mig_ptr(). the address will
 * reference @dpos within @dst, which must be within @dst->max_probe slots of
 * the initial position given by @hash.
 *
 * the format consists of @dst->probe_addr_size_log2 bits of @dpos and an
 * overflow bit after it. mig_slot() adds the overflow bit's value to @hash
 * before bits from it are injected to form a slot number.
 */
static inline uintptr_t probe_addr(
	size_t dpos, size_t hash, struct lfht_table *dst)
{
	uintptr_t bit = 1ul << dst->probe_addr_size_log2;
	return ((dpos & bit) != (hash & bit) ? bit : 0) | (dpos & (bit - 1));
}


static inline uintptr_t mig_ptr(
	const struct lfht_table *t,
	unsigned long ref_gen_id, uintptr_t probe_addr)
{
	assert(ref_gen_id > t->gen_id);
	uintptr_t raw = (ref_gen_id - t->gen_id) | (probe_addr << 10),
		e = t->mig_bit | (raw & (t->mig_bit - 1))
			| (raw & ~(t->mig_bit - 1)) << 1;
	assert(mig_gen_id(t, e) == ref_gen_id);
	return e;
}


static inline struct lfht_table *get_main(const struct lfht *lfht) {
	return container_of_or_null(nbsl_top(&lfht->tables),
		struct lfht_table, link);
}


static inline struct lfht_table *get_next(const struct lfht_table *tab) {
	return nbsl_next_node(&tab->link, struct lfht_table, link);
}


static void mig_deref(
	const struct lfht *ht, size_t hash,
	struct lfht_table **tab_p, size_t *pos_p, uintptr_t *val_p,
	uintptr_t migptr)
{
	assert(mig_gen_id(*tab_p, migptr) > 0);
	unsigned long gen_id = mig_gen_id(*tab_p, migptr);
	struct lfht_table *cand = get_main(ht);
	while(cand != NULL && cand->gen_id > gen_id) {
		cand = get_next(cand);
	}
	assert(cand != NULL);
	assert(cand->gen_id == gen_id);

	size_t pos = mig_slot(*tab_p, hash, migptr, cand);
	assert(pos >= 0 && pos < (1ul << cand->size_log2));
	*tab_p = cand;
	*pos_p = pos;
	*val_p = atomic_load_explicit(&cand->table[pos], memory_order_relaxed);
	assert(is_val(cand, *val_p)
		|| (*val_p & cand->mig_bit) != 0
		|| (*val_p & cand->del_bit) != 0);
}


static inline uintptr_t take_bit(uintptr_t *set)
{
	/* deviate from CCAN htable by preferring very high-order bits. could
	 * replace MSB(*set) with ffsl(*set) - 1 to do the opposite, but why
	 * bother?
	 */
	assert(*set != 0);
	uintptr_t v = (uintptr_t)1 << MSB(*set);
	assert((*set & v) != 0);
	*set &= ~v;
	return v;
}


static void set_resv_bits(struct lfht_table *tab)
{
	uintptr_t cm = tab->common_mask;
	if(likely(POPCOUNT(cm) >= 5)) {
		tab->ephem_bit = take_bit(&cm);
		tab->src_bit = take_bit(&cm);
		tab->del_bit = take_bit(&cm);
		tab->mig_bit = take_bit(&cm);
		tab->hazard_bit = take_bit(&cm);
	} else {
		/* FIXME: the ugly fix for this involves making up all new common_mask
		 * and common_bits, for an equal-sized main table as the previous, but
		 * with 5 or 6 bits to spare. this'd use allocated storage to keep
		 * combinations of keys that have no common bits, and require some
		 * adjustments to the migration algorithm. so for now let's leave it
		 * in a "tests don't hit it" condition.
		 */
		assert("migration requires 5 special bits (FIXME)" == NULL);
	}
	tab->perfect_bit = cm != 0 ? take_bit(&cm) : 0;
	tab->resv_mask = tab->perfect_bit | tab->ephem_bit | tab->src_bit
		| tab->del_bit | tab->mig_bit | tab->hazard_bit;
}


static void set_bits(
	int first_size_log2,
	struct lfht_table *tab, const struct lfht_table *prev,
	void *model)
{
	if(prev == NULL) {
		/* punch the initial size's worth of holes in the common mask above
		 * a typical malloc grain of 32 bytes.
		 */
		if(first_size_log2 < MIN_SIZE_LOG2 + 4) {
			first_size_log2 = MIN_SIZE_LOG2 + 4;
		}
		tab->common_mask = (~(uintptr_t)0 << first_size_log2) | 0x1f;
		assert(model != NULL);
		tab->common_bits = (uintptr_t)model & tab->common_mask;
		set_resv_bits(tab);
	} else {
		tab->common_mask = prev->common_mask;
		tab->common_bits = prev->common_bits;

		uintptr_t m = (uintptr_t)model;
		if(model != NULL && (m & tab->common_mask) != tab->common_bits) {
			/* reduce common_mask, recompute reserved bits. */
			uintptr_t new = tab->common_bits ^ (m & tab->common_mask);
			assert((new & tab->common_mask) != 0);
			tab->common_mask &= ~new;
			tab->common_bits &= ~new;
			assert((m & tab->common_mask) == tab->common_bits);
			set_resv_bits(tab);
		} else {
			/* inherit reserved bits. */
			tab->resv_mask = prev->resv_mask;
			tab->perfect_bit = prev->perfect_bit;
			tab->ephem_bit = prev->ephem_bit;
			tab->src_bit = prev->src_bit;
			tab->del_bit = prev->del_bit;
			tab->mig_bit = prev->mig_bit;
			tab->hazard_bit = prev->hazard_bit;
		}
	}

	assert((tab->common_bits & ~tab->common_mask) == 0);
	assert(model == NULL
		|| ((uintptr_t)model & tab->common_mask) == tab->common_bits);

	/* (could check the same about the other reserved bits, but meh.) */
	assert(POPCOUNT(tab->perfect_bit) <= 1);
	assert(tab->perfect_bit == 0
		|| (tab->perfect_bit & tab->common_mask) != 0);

	assert(POPCOUNT(tab->resv_mask) == 5 || POPCOUNT(tab->resv_mask) == 6);
	assert((tab->common_mask & tab->resv_mask) == tab->resv_mask);
	assert(POPCOUNT(tab->resv_mask) <= POPCOUNT(tab->common_mask));
}


static void get_totals(
	size_t *elems_p, size_t *deleted_p, size_t *mig_left_p,
	struct lfht_table *t)
{
	atomic_thread_fence(memory_order_acquire);
	size_t e = 0, d = 0;
	ssize_t ml = 0;
	for(int base = sched_getcpu() >> t->pc->shift, i = 0;
		i < t->pc->n_buckets;
		i++)
	{
		struct lfht_table_percpu *pc = percpu_get(t->pc, base ^ i);
		e += atomic_load_explicit(&pc->elems, memory_order_relaxed);
		d += atomic_load_explicit(&pc->deleted, memory_order_relaxed);
		if(mig_left_p != NULL) {
			ml += atomic_load_explicit(&pc->mig_left, memory_order_relaxed);
		}
	}
	*elems_p = e;
	*deleted_p = d;
	if(mig_left_p != NULL) *mig_left_p = ml;
}


static inline size_t get_total_elems(struct lfht_table *t) {
	size_t e, d;
	get_totals(&e, &d, NULL, t);
	return e;
}


static inline size_t get_total_mig_left(struct lfht_table *t) {
	size_t e, d, ml;
	get_totals(&e, &d, &ml, t);
	return ml;
}


/* FIXME: handle the case where gen_id wraps around by compressing gen_ids
 * from far up. this is rather unlikely to matter for now, but is absolutely
 * critical for multi-year stability, since rehashing will continue
 * indefinitely in a lfht under load. (this comment is here because setting
 * gen_id happens at the new_table() callsites, which are several.)
 */
static struct lfht_table *new_table(int sizelog2)
{
	assert(sizelog2 >= MIN_SIZE_LOG2);
	struct lfht_table *tab = aligned_alloc(
		alignof(struct lfht_table), sizeof(*tab));
	if(tab == NULL) return NULL;
	tab->link.next = 0;
	tab->size_log2 = sizelog2;
	tab->gen_id = 0;
	tab->table = calloc(1L << sizelog2, sizeof(uintptr_t));
	if(tab->table == NULL) {
		free(tab);
		return NULL;
	}
	tab->pc = percpu_new(sizeof(struct lfht_table_percpu), NULL);
	if(tab->pc == NULL) {
		free(tab->table);
		free(tab);
		return NULL;
	}

	/* from CCAN htable */
	tab->max = ((size_t)3 << sizelog2) / 4;
	tab->max_with_deleted = ((size_t)9 << sizelog2) / 10;

	/* maximum probe distance is statically limited to 128k by the migration
	 * pointer format on 32-bit targets, and arbitrarily capped to 4/5ths of
	 * table size.
	 *
	 * TODO: better formulas exist. apply them.
	 */
	tab->max_probe = (4ul << tab->size_log2) / 5;
	if(tab->max_probe < MIN_PROBE) tab->max_probe = MIN_PROBE;
	else if(tab->max_probe > 128 * 1024) tab->max_probe = 128 * 1024;
	tab->probe_addr_size_log2 = size_to_log2(tab->max_probe * 2);
	assert((1ul << tab->probe_addr_size_log2) >= tab->max_probe * 2);
	assert(tab->probe_addr_size_log2 <= sizeof(uintptr_t) * 8 - 11);

	/* assign migration chunks. */
	size_t remain = 1ul << sizelog2,
		chunk = remain / tab->pc->n_buckets;
	for(int i=0; i < tab->pc->n_buckets; i++) {
		struct lfht_table_percpu *p = percpu_get(tab->pc, i);
		p->mig_next = remain - 1;
		if(i == tab->pc->n_buckets - 1) {
			p->mig_left = remain;
		} else {
			assert(remain > chunk);
			p->mig_left = chunk;
		}
		remain -= p->mig_left;
		p->mig_last = remain;
	}
	assert(remain == 0);

	atomic_thread_fence(memory_order_release);
	return tab;
}


/* try to install a new main table until the main table's common mask & bits
 * accommodate @model. returns NULL on malloc() failure.
 */
static struct lfht_table *remask_table(
	struct lfht *ht, struct lfht_table *tab, void *model)
{
	assert(model != NULL);

	struct lfht_table *nt = new_table(tab->size_log2);
	if(nt == NULL) return NULL;

	for(;;) {
		set_bits(0, nt, tab, model);
		nt->gen_id = tab->gen_id + 1;
		if(nbsl_push(&ht->tables, &tab->link, &nt->link)) {
			/* i won! i won! */
			return nt;
		}
		tab = get_main(ht);
		if(((uintptr_t)model & tab->common_mask) == tab->common_bits) {
			/* concurrently replaced with a conforming table, superceding
			 * ours.
			 */
			free(nt->table);
			free(nt);
			return tab;
		} else if(tab->size_log2 > nt->size_log2) {
			/* concurrently doubled. reallocate ours & retry. */
			free(nt->table);
			free(nt);
			nt = new_table(tab->size_log2);
			if(nt == NULL) return NULL;
		} else {
			/* concurrent remask or rehash. retry w/ same new table. */
		}
	}
}


/* install a new table, twice the size of @tab, in @ht. if malloc fails,
 * return NULL. if replacement fails, and the new one is larger than @tab,
 * return that; if it's not larger, redo with that instead of @tab.
 */
static struct lfht_table *double_table(
	struct lfht *ht, struct lfht_table *tab, void *model)
{
	struct lfht_table *nt = new_table(tab->size_log2 + 1);
	if(nt == NULL) return NULL;

	for(;;) {
		set_bits(0, nt, tab, model);
		nt->gen_id = tab->gen_id + 1;
		if(nbsl_push(&ht->tables, &tab->link, &nt->link)) return nt;
		tab = get_main(ht);
		if(tab->size_log2 >= nt->size_log2) {
			/* resized by another thread. */
			free(nt->table);
			free(nt);
			break;
		}
		/* was replaced by rehash. doubling remains appropriate. */
	}

	if(model != NULL
		&& ((uintptr_t)model & tab->common_mask) != tab->common_bits)
	{
		/* replace it w/ same size, but conformant. */
		return remask_table(ht, tab, model);
	} else {
		/* it's acceptable. */
		return tab;
	}
}


/* install a new table of exactly the same size. lfht_add() will migrate two
 * items at a time while the new table remains @ht's main table. if malloc
 * fails, return @tab; if switching fails, return the new table.
 */
static struct lfht_table *rehash_table(
	struct lfht *ht, struct lfht_table *tab)
{
	struct lfht_table *nt = new_table(tab->size_log2);
	if(nt == NULL) return tab;
	set_bits(0, nt, tab, NULL);
	nt->gen_id = tab->gen_id + 1;
	if(nbsl_push(&ht->tables, &tab->link, &nt->link)) tab = nt;
	else {
		free(nt->table);
		free(nt);
		tab = get_main(ht);
	}
	return tab;
}


static void table_dtor(struct lfht_table *tab)
{
	assert(get_total_elems(tab) == 0);
	percpu_free(tab->pc);
	free(tab->table);
	free(tab);
}


static void remove_table(struct lfht *ht, struct lfht_table *tab)
{
	assert(tab != NULL);
	if(nbsl_del(&ht->tables, &tab->link)) {
		e_call_dtor(&table_dtor, tab);
	}
}


static inline uintptr_t make_hval(
	const struct lfht_table *tab, const void *p, uintptr_t bits)
{
	return ((uintptr_t)p & ~tab->common_mask) | bits;
}


static inline uintptr_t get_hash_ptr_bits(
	const struct lfht_table *tab, size_t hash)
{
	/* mixes @hash back into itself to utilize the size_log2 bits that'd
	 * otherwise be disregarded, and to spread a 32-bit hash into the extra
	 * bits on 64-bit hosts. deviates from CCAN htable by rotating to the
	 * right, whereas CCAN does a simple shift.
	 */
	int n = tab->size_log2 + 4;
	return (hash ^ ((hash >> n) | (hash << (sizeof(hash) * 8 - n))))
		& tab->common_mask & ~tab->resv_mask;
}


static inline uintptr_t get_extra_ptr_bits(
	const struct lfht_table *tab, uintptr_t e)
{
	return e & tab->common_mask & ~(tab->resv_mask & ~tab->perfect_bit);
}


static inline void *get_raw_ptr(const struct lfht_table *tab, uintptr_t e) {
	return (void *)((e & ~tab->common_mask) | tab->common_bits);
}


/* returns nonnegative slot index in @tab->table on success, -EAGAIN to
 * indicate that the caller should reload @tab, and -ENOSPC when probe
 * distance was exceeded. *@new_entry_p is filled in on success, and may be
 * written to on failure. skips over slots that have @tab->hazard_bit set iff
 * @extra_bits contains @tab->ephem_bit. decrements DELETED(@tab) iff the slot
 * written to was a deleted row, and never increments ELEMS(@tab).
 */
static ssize_t ht_add(
	uintptr_t *new_entry_p,
	struct lfht_table *tab, const void *p, size_t hash,
	uintptr_t extra_bits)
{
	assert(new_entry_p != NULL);
	assert(((uintptr_t)p & tab->common_mask) == tab->common_bits);
	assert((extra_bits & tab->hazard_bit) == 0);

	uintptr_t perfect = tab->perfect_bit;
	size_t mask = (1ul << tab->size_log2) - 1, start = hash & mask,
		end = (start + tab->max_probe) & mask,
		i = start;
	do {
		uintptr_t e = atomic_load_explicit(&tab->table[i],
			memory_order_relaxed);
retry:
		if(!is_empty(tab, e)) {
			if((e & tab->mig_bit) != 0) {
				/* optimization: migration implies @tab is secondary, so
				 * ht_add() should be tried again on the primary. this avoids
				 * an eventual off-cpu migration.
				 */
				return -EAGAIN;
			}
			/* slot was occupied; go on to the next one. */
		} else if((e & tab->hazard_bit) == 0
			|| (extra_bits & tab->ephem_bit) == 0)
		{
			uintptr_t hval = make_hval(tab, p,
				get_hash_ptr_bits(tab, hash) | perfect
					| extra_bits | (e & tab->hazard_bit));
			*new_entry_p = hval;
			assert(is_val(tab, hval));
			assert((e & tab->hazard_bit) == (hval & tab->hazard_bit));
			if(!atomic_compare_exchange_strong_explicit(
				&tab->table[i], &e, hval,
				memory_order_release, memory_order_relaxed))
			{
				/* slot was snatched; go again. */
				goto retry;
			}

			if(e == tab->del_bit) {
				atomic_fetch_sub_explicit(&DELETED(tab), 1,
					memory_order_relaxed);
			}
			assert(i >= 0 && i <= SSIZE_MAX);
			return i;
		}
		i = (i + 1) & mask;
		perfect = 0;
	} while(i != end);
	return -ENOSPC;
}


/* NOTE: the perfect-bit handling here looks wrong, but that's because
 * @it->perfect is cleared in lfht_nextval(). this is just a tiny bit more
 * microefficient.
 */
static void *ht_val(
	const struct lfht *ht, struct lfht_iter *it, size_t hash)
{
	uintptr_t mask = (1ul << it->t->size_log2) - 1,
		perfect = it->perfect,
		h2 = get_hash_ptr_bits(it->t, hash) | perfect;
	do {
		uintptr_t e = atomic_load_explicit(&it->t->table[it->off],
			memory_order_relaxed);
		if(is_void(it->t, e)) break;
		if(is_val(it->t, e) && get_extra_ptr_bits(it->t, e) == h2) {
			return get_raw_ptr(it->t, e);
		}
		it->off = (it->off + 1) & mask;
		h2 &= ~perfect;
	} while(it->off != it->end);

	return NULL;
}


static ssize_t ht_mig_filter(
	struct lfht_table *src, ssize_t spos, uintptr_t *entry_p)
{
	assert(spos >= 0);
	uintptr_t e = *entry_p;

retry:
	if(!is_val(src, e)) {
		/* clear an empty slot. */
		uintptr_t new = e == 0 ? mig_void(src) : mig_val(src);
		assert(!is_val(src, new));
		if(atomic_compare_exchange_strong_explicit(&src->table[spos],
			&e, new, memory_order_release, memory_order_relaxed))
		{
			/* success. */
			return 0;
		} else {
			/* concurrent modification. */
			if((e & src->mig_bit) != 0) {
				/* skip. */
				assert(src->halt_gen_id > 0);
				return -1;
			} else {
				/* reload and retry. */
				*entry_p = e;
				goto retry;
			}
		}
	} else if((e & src->mig_bit) != 0 || (e & src->src_bit) != 0) {
		/* in a table where migration was previously halted, rows may be
		 * encountered where migration has either completed or is in progress.
		 * they should be skipped.
		 */
		assert(src->halt_gen_id > 0);
		return -1;
	} else {
		/* it's a valid migration source; proceed. */
		return 1;
	}
}


static ssize_t ht_mig_mark_and_copy(
	size_t *hash_p, uintptr_t *dstval_p,
	struct lfht *ht, struct lfht_table *dst,
	struct lfht_table *src, struct lfht_table_percpu *src_pc, ssize_t spos,
	uintptr_t *entry_p)
{
	uintptr_t e = *entry_p;

	assert((e & src->src_bit) == 0);
	assert((e & src->del_bit) == 0);
	assert((e & src->mig_bit) == 0);
	if(!atomic_compare_exchange_strong(
		&src->table[spos], &e, e | src->src_bit))
	{
		/* `e' was concurrently updated. retry. */
		*entry_p = e;
		return -EINVAL;
	}
	e |= src->src_bit;

	/* source entry marked, add dst copy. */
	void *ptr = get_raw_ptr(src, e);
	size_t hash = (*ht->rehash_fn)(ptr, ht->priv);
	*hash_p = hash;
	ssize_t n = ht_add(dstval_p, dst, ptr, hash, dst->ephem_bit);
	if(unlikely(n < 0)) {
		/* failure path. unmark source entry. */
		uintptr_t old_e = e;
		if(!atomic_compare_exchange_strong(
			&src->table[spos], &e, e & ~src->src_bit))
		{
			assert(e == (old_e | src->del_bit));
			/* concurrent deletion was indicated; bash the slot like
			 * ht_mig_filter().
			 */
			atomic_store(&src->table[spos], mig_val(src));
			atomic_fetch_sub(&src_pc->elems, 1);
			*entry_p = mig_val(src);
			return -ENOENT;
		}
		e &= ~src->src_bit;
	} else {
		atomic_fetch_add(&ELEMS(dst), 1);
	}

	*entry_p = e;
	return n;
}


/* clears the ephemeral bit while completing another migrator's latent
 * deletion by `sed → mig_val(@dst)'. return value is *@dstval_p's mig_bit
 * cast to boolean.
 */
static bool ht_mig_clear_e(
	struct lfht_table *dst, size_t dpos, uintptr_t *dstval_p)
{
	while((*dstval_p & dst->mig_bit) == 0) {
		assert((*dstval_p & dst->hazard_bit) == 0);
		uintptr_t state = *dstval_p & (dst->resv_mask & ~dst->perfect_bit),
			newval;
		if(likely(state == dst->ephem_bit)) {
			/* clear e, set h. */
			newval = (*dstval_p & ~dst->ephem_bit) | dst->hazard_bit;
		} else if(state == (dst->ephem_bit | dst->del_bit)) {
			/* late deletion of ephemeral item. */
			newval = dst->del_bit | dst->hazard_bit;
		} else if(state == (dst->ephem_bit | dst->src_bit | dst->del_bit)) {
			/* clear the marker state for late deletion of ephemeral row while
			 * it was a migration source. this can happen.
			 */
			newval = mig_val(dst);
		} else {
			assert("expected state e, ed, or sed" == NULL);
		}
		assert(!is_void(dst, newval));
		if(atomic_compare_exchange_strong(
			&dst->table[dpos], dstval_p, newval))
		{
			if((newval & (dst->mig_bit | dst->del_bit)) != 0) {
				atomic_fetch_sub(&ELEMS(dst), 1);
			}
			*dstval_p = newval;
			break;
		}
	}
	return (*dstval_p & dst->mig_bit) != 0;
}


/* clears the hazard bit iff *@dstval_p has mig_bit clear. */
static void ht_mig_clear_h(
	struct lfht_table *dst, size_t dpos, uintptr_t *dstval_p)
{
	while((*dstval_p & dst->mig_bit) == 0) {
		/* (this assert blowing indicates that someone, somewhere, didn't
		 * pass the hazard bit through correctly when writing at @dpos.)
		 */
		assert((*dstval_p & dst->hazard_bit) != 0);
		assert(!is_void(dst, *dstval_p & ~dst->hazard_bit));
		if(atomic_compare_exchange_strong(&dst->table[dpos],
			dstval_p, *dstval_p & ~dst->hazard_bit))
		{
			/* done. */
			break;
		}
	}
}


/* decrements mig_left. @src_pc must be the percpu structure that
 * take_mig_work() returned. @last_chunk may be the value from
 * take_mig_work(), or false where not applicable.
 *
 * returns true when @src was removed from @ht, false otherwise.
 */
static bool ht_mig_advance(
	struct lfht *ht, struct lfht_table *src, struct lfht_table_percpu *src_pc,
	bool last_chunk)
{
	if(atomic_fetch_sub_explicit(&src_pc->mig_left, 1,
			memory_order_relaxed) == 1
		&& (last_chunk || get_total_mig_left(src) == 0))
	{
		/* emptied the table. it can now be removed. */
		assert(src_pc->mig_next < src_pc->mig_last || src->halt_gen_id > 0);
		remove_table(ht, src);
		return true;
	} else {
		/* go on. */
		return false;
	}
}


/* returns 0 on success, -1 when the row was skipped or delayed (i.e. in the
 * chain-creation case) and shouldn't be counted off mig_left by caller.
 */
static int ht_mig_resolve(
	struct lfht *ht, size_t hash,
	struct lfht_table *dst, size_t dpos, uintptr_t dstval,
	struct lfht_table *src, struct lfht_table_percpu *src_pc, size_t spos,
	uintptr_t srcval)
{
	assert((dstval & dst->ephem_bit) != 0);
	uintptr_t ptr = mig_ptr(src, dst->gen_id, probe_addr(dpos, hash, dst)),
		bits;
	assert(mig_slot(src, hash, ptr, dst) == dpos);
	assert(mig_gen_id(src, ptr) == dst->gen_id);

retry:
	bits = srcval & (src->src_bit | src->ephem_bit | src->del_bit);
	if(likely(bits == src->src_bit)) {
		/* unconflicted case. store the migration source pointer, making the
		 * source row no longer subject to late deletion and permitting late
		 * deletion of the destination row.
		 */
		assert(!is_void(src, ptr));
		if(!atomic_compare_exchange_strong(&src->table[spos], &srcval, ptr)) {
			goto retry;
		}
		atomic_fetch_sub(&src_pc->elems, 1);
		bool dst_is_mig, chain_root = true;
chain_retry:
		dst_is_mig = ht_mig_clear_e(dst, dpos, &dstval);
		atomic_store(&src->table[spos], mig_val(src));
		if(likely(!dst_is_mig) || mig_gen_id(dst, dstval) == 0) {
			/* ordinary case. clear hazard bit and finish. */
			assert((dstval & dst->hazard_bit) != 0);
			ht_mig_clear_h(dst, dpos, &dstval);
		} else {
			/* walking down the chain. */
			assert(dst != get_main(ht));
			assert(src->gen_id < dst->gen_id);
			if(!chain_root) ht_mig_advance(ht, src, src_pc, false);
			chain_root = false;
			src = dst; spos = dpos; srcval = dstval; src_pc = MY_PERCPU(src);
			mig_deref(ht, hash, &dst, &dpos, &dstval, srcval);
			assert(dst != src);
			assert(src->gen_id < dst->gen_id);
			goto chain_retry;
		}
		return 0;
	} else if(bits == (src->src_bit | src->ephem_bit)) {
		/* chaining case. */
		assert(!is_void(src, ptr));
		if(!atomic_compare_exchange_strong(&src->table[spos], &srcval, ptr)) {
			goto retry;
		}
		atomic_fetch_sub(&src_pc->elems, 1);
		return -1;	/* don't count off from mig_left */
	} else {
		assert((bits & src->del_bit) != 0);
		/* deleted case. */
		uintptr_t newdst;

del_retry:
		/* remove dest row, perhaps latently. */
		assert((dstval & dst->del_bit) == 0);
		newdst = dst->del_bit;
		if((dstval & dst->src_bit) != 0) newdst |= dstval;
		uintptr_t olddst = dstval;
		assert(!is_void(dst, newdst));
		if(!atomic_compare_exchange_strong(
			&dst->table[dpos], &dstval, newdst))
		{
			assert((olddst & dst->src_bit) == 0);
			/* (this one blows up when dstval has been late deleted while
			 * ephemeral but source existed, such as when a mig pointer fuckup
			 * causes wrong matching.)
			 */
			assert(dstval == (olddst | dst->src_bit));
			goto del_retry;
		}
		atomic_fetch_sub(&DELETED(dst), 1);

		/* remove source. */
		atomic_store(&src->table[spos], mig_val(src));
		atomic_fetch_sub(&src_pc->elems, 1);
		return 0;
	}
}


/* migrate a single entry from @src at @spos into @dst. performs migration to
 * @dst (or @ht's main table) if the entry is valid, leaving the slot with
 * mig_bit set regardless.
 *
 * returns 0 on successful migration, <0 to skip the row, and >0 to skip the
 * table (as in the NOSPC case).
 */
static int ht_migrate_entry(
	struct lfht *ht, struct lfht_table *dst,
	struct lfht_table *src, struct lfht_table_percpu *src_pc, ssize_t spos)
{
	assert(spos >= 0);
	ssize_t n;
	size_t hash;
	uintptr_t e = atomic_load_explicit(&src->table[spos],
		memory_order_relaxed), dstval;

e_retry:
	n = ht_mig_filter(src, spos, &e);
	if(n <= 0) return n;	/* simple completion and skipping. */

dst_retry:
	n = ht_mig_mark_and_copy(&hash, &dstval, ht, dst, src, src_pc, spos, &e);
	if(unlikely(n < 0)) {
		struct lfht_table *rarest = get_main(ht);
		if(n == -EINVAL) goto e_retry;	/* `e' was reloaded. try again. */
		else if(n == -EAGAIN) {
			/* @dst was under migration. refetch @dst and try again. */
			assert(rarest != dst);
			dst = rarest;
			goto dst_retry;
		} else if(n == -ENOSPC) {
			/* copy couldn't be inserted because probe length was exceeded.
			 *
			 * most of the time, hash chains in @src should be as long or
			 * shorter when moved into @dst, but it's possible for items added
			 * to @dst to increase a chain past that limit, particularly with
			 * rehash and remask tables. this breaks migration.
			 *
			 * the solution used here halts migration of this table until the
			 * primary table becomes something besides @dst. interrupted
			 * migration is noted in @src->mig_next, causing migrated rows to
			 * possibly appear in ht_migrate_entry().
			 */
			if(rarest != dst) {
				/* though, try again in the main table if distinct. */
				dst = rarest;
				goto dst_retry;
			} else {
				increase_to(&src->halt_gen_id, dst->gen_id);
				increase_to(&src_pc->mig_next, spos);
				/* skip @src; migration may succeed from elsewhere. */
				return 1;
			}
		} else {
			assert(n == -ENOENT);
			/* same as above, but the source row was concurrently deleted
			 * during the ht_mig_mark_and_copy() error path. this is success
			 * per ht_mig_filter()'s interface.
			 */
			return 0;		/* completed. */
		}
	}

	return ht_mig_resolve(ht, hash, dst, n, dstval, src, src_pc, spos, e);
}


static ssize_t take_percpu_work(struct lfht_table_percpu *c)
{
	ssize_t next = atomic_load_explicit(&c->mig_next,
		memory_order_relaxed);
	while(next >= c->mig_last
		&& !atomic_compare_exchange_weak(&c->mig_next, &next, next - 1))
	{
		/* spin */
	}
	return next < c->mig_last ? -1 : next;
}


static ssize_t take_mig_work(
	bool *last_p,
	struct lfht_table_percpu **pc_p,
	struct lfht_table *src)
{
	ssize_t work = -1;
	for(int base = sched_getcpu() >> src->pc->shift, i = 0;
		i < src->pc->n_buckets && work < 0;
		i++)
	{
		struct lfht_table_percpu *c = percpu_get(src->pc, base ^ i);
		work = take_percpu_work(c);
		if(work >= 0) {
			*pc_p = c;
			*last_p = (i == src->pc->n_buckets - 1);
		}
	}

	return work;
}


/* driver function of the migration operation. gets a work slot in @src and
 * passes it along to ht_migrate_entry(), removing tables as migration
 * completes.
 *
 * returns true when @src became empty, was already empty, or migration was
 * blocked on it.
 */
static bool ht_migrate_once(
	struct lfht *ht, struct lfht_table *dst, struct lfht_table *src)
{
	struct lfht_table_percpu *src_pc;
	bool last_chunk;
	for(;;) {
		ssize_t spos = take_mig_work(&last_chunk, &src_pc, src);
		if(spos < 0) return true;	/* skip table (completed) */

		int n = ht_migrate_entry(ht, dst, src, src_pc, spos);
		if(n == 0) break;	/* ok */
		else if(n > 0) return true;	/* skip table (blocked) */
		assert(n < 0);	/* skip row (take again) */
	}

	/* check it off and test for completion. */
	return ht_mig_advance(ht, src, src_pc, last_chunk);
}


/* examine and possibly migrate one entry from a smaller secondary table into
 * @ht's main table (double), or three from an equal-sized secondary table or
 * if there's more than one secondary table (rehash/remask).
 *
 * the doubling of size ensures that the secondary is emptied by the time the
 * primary fills up, and the doubling threshold's kicking in at 3/4 full means
 * a 3:1 ratio will achieve the same for rehash/remask (though significantly
 * ahead of time).
 */
static void ht_migrate(struct lfht *ht, struct lfht_table *dst)
{
	bool single = true;
	struct lfht_table *sec = NULL, *next = dst;
	for(;;) {
		next = get_next(next);
		if(next == NULL) break;
		single = false;
		unsigned long halt_gen = atomic_load_explicit(
			&next->halt_gen_id, memory_order_relaxed);
		if(halt_gen < dst->gen_id) sec = next;
	}
	if(sec == NULL) return;		/* nothing to do! */

	int n_times = dst->size_log2 > sec->size_log2 && single ? 1 : 3;
	for(int i=0; i < n_times; i++) {
		if(ht_migrate_once(ht, dst, sec) && n_times > 1) {
			sec = next_table_gen(ht, sec, true);
			if(sec == NULL || sec->gen_id >= dst->gen_id) {
				assert(sec == NULL
					|| sec->gen_id > dst->gen_id
					|| sec == dst);
				break;
			}
		}
	}
}


void lfht_init(
	struct lfht *ht,
	size_t (*rehash_fn)(const void *ptr, void *priv), void *priv)
{
	struct lfht t = LFHT_INITIALIZER(t, rehash_fn, priv);
	*ht = t;
}


void lfht_init_sized(
	struct lfht *ht,
	size_t (*rehash_fn)(const void *ptr, void *priv), void *priv,
	size_t size)
{
	lfht_init(ht, rehash_fn, priv);
	int sizelog2 = MIN_SIZE_LOG2;
	while(1UL << sizelog2 < size) {
		sizelog2++;
		if(sizelog2 == sizeof(long) * 8 - 1) break;
	}
	ht->first_size_log2 = sizelog2;
}


void lfht_clear(struct lfht *ht)
{
	int eck = e_begin();
	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(&ht->tables, &it);
		cur != NULL;
		cur = nbsl_next(&ht->tables, &it))
	{
		struct lfht_table *tab = container_of(cur, struct lfht_table, link);
		if(!nbsl_del_at(&ht->tables, &it)) continue;
		e_free(tab->table);
		e_free(tab);
	}
	e_end(eck);
}


bool lfht_add(struct lfht *ht, size_t hash, void *p)
{
	int eck = e_begin();

	struct lfht_table *tab = get_main(ht);
	if(unlikely(tab == NULL)) {
		tab = new_table(ht->first_size_log2);
		if(tab == NULL) goto fail;
		set_bits(ht->first_size_log2, tab, NULL, p);
		if(!nbsl_push(&ht->tables, NULL, &tab->link)) {
			free(tab->table);
			free(tab);
			tab = get_main(ht);
		}
	}

	ssize_t n;
	do {
		if(((uintptr_t)p & tab->common_mask) != tab->common_bits) {
			tab = remask_table(ht, tab, p);
			if(tab == NULL) goto fail;
			assert(((uintptr_t)p & tab->common_mask) == tab->common_bits);
		}

		uintptr_t new_entry;
		n = ht_add(&new_entry, tab, p, hash, 0);
		if(n == -EAGAIN) {
			/* @tab became secondary. reload tab'=main and retry to avoid a
			 * future off-cpu migration.
			 */
			tab = get_main(ht);
		} else if(n == -ENOSPC) {
			/* probe limit was reached. double or rehash the table. */
			size_t elems, deleted;
			get_totals(&elems, &deleted, NULL, tab);
			if(elems + 1 <= tab->max
				&& elems + 1 + deleted > tab->max_with_deleted)
			{
				tab = rehash_table(ht, tab);
				assert(tab != NULL);
			} else {
				tab = double_table(ht, tab, p);
				if(tab == NULL) goto fail;
			}
		}
	} while(n < 0);
	assert(n >= 0);
	atomic_fetch_add_explicit(&ELEMS(tab), 1, memory_order_relaxed);
	ht_migrate(ht, tab);

	e_end(eck);
	return true;

fail:
	e_end(eck);
	return false;
}


/* for all next tables of @dst, find a migration pointer within the probe area
 * of @hash that points to @dpos within @dst; or find a source-marked entry
 * that matches the one in @dst->table[@dpos] and mark it for late deletion.
 *
 * returns 0 when an entry was found, -ENOENT when it wasn't, and >0 when
 * del_bit was set in a matching source entry.
 */
static int hunt_mig_ptr(
	struct lfht_table *dst, size_t dpos, size_t hash, void *p)
{
	assert(dpos <= SSIZE_MAX);
	size_t p_addr = probe_addr(dpos, hash, dst);
	for(struct lfht_table *s = get_next(dst); s != NULL; s = get_next(s)) {
		uintptr_t ptr = mig_ptr(s, dst->gen_id, p_addr);
		size_t mask = (1ul << s->size_log2) - 1,
			pos = hash & mask, end = (pos + s->max_probe) & mask;
		do {
			uintptr_t e = atomic_load_explicit(&s->table[pos],
				memory_order_relaxed);
e_retry:
			if(e == ptr) {
				assert(mig_slot(s, hash, e, dst) == dpos);
				assert(mig_gen_id(s, e) == dst->gen_id);
				return 0;
			} else if(is_void(s, e)) break;
			else if((e & s->src_bit) != 0 && get_raw_ptr(s, e) == p
				&& (e & (s->ephem_bit | s->del_bit)) == 0)
			{
				if(!atomic_compare_exchange_strong(&s->table[pos],
					&e, e | s->del_bit))
				{
					/* consume it again. */
					goto e_retry;
				}
				return 1;
			}
			pos = (pos + 1) & mask;
		} while(pos != end);
	}
	return -ENOENT;
}


bool lfht_delval(const struct lfht *ht, struct lfht_iter *it, void *p)
{
	assert(e_inside());

	uintptr_t e = atomic_load_explicit(&it->t->table[it->off],
		memory_order_relaxed), new_e;
	do {
		if(!is_val(it->t, e) || get_raw_ptr(it->t, e) != p) {
			/* plz no step on snek */
			return false;
		}

		new_e = it->t->del_bit | (e & it->t->hazard_bit);
		if((e & it->t->src_bit) != 0) {
			assert((e & it->t->del_bit) == 0);	/* per is_val() */
			new_e |= e;
		} else if((e & it->t->ephem_bit) != 0) {
			/* hunt for a compatible MIG entry. if found, set `d'. if a source
			 * entry matching @p became marked for late deletion, succeed
			 * immediately.
			 */
			int n = hunt_mig_ptr(it->t, it->off, it->hash, p);
			if(likely(n == 0)) new_e |= e;
			else if(n > 0) return true;
			else {
				return false;
			}
		}
	} while(!atomic_compare_exchange_strong_explicit(
		&it->t->table[it->off], &e, new_e,
		memory_order_relaxed, memory_order_relaxed));
	assert((e & it->t->hazard_bit) == (new_e & it->t->hazard_bit));

	if((e & (it->t->src_bit | it->t->ephem_bit)) == 0) {
		struct lfht_table_percpu *pc = MY_PERCPU(it->t);
		atomic_fetch_add_explicit(&pc->deleted, 1, memory_order_relaxed);
		atomic_fetch_sub_explicit(&pc->elems, 1, memory_order_acq_rel);
	}

	return true;
}


bool lfht_del(struct lfht *ht, size_t hash, const void *p)
{
	int eck = e_begin();
	bool found = false;
	struct lfht_iter it;
	for(void *c = lfht_firstval(ht, &it, hash);
		c != NULL;
		c = lfht_nextval(ht, &it, hash))
	{
		if(c == p && lfht_delval(ht, &it, c)) {
			found = true;
			break;
		}
	}
	e_end(eck);
	return found;
}


/* initialize for a new @tab. */
static inline void lfht_iter_init(
	struct lfht_iter *it, struct lfht_table *tab, size_t hash)
{
	size_t mask = (1ul << tab->size_log2) - 1;
	it->t = tab;
	it->off = hash & mask;
	it->end = (it->off + tab->max_probe) & mask;
	it->hash = hash;
	it->perfect = tab->perfect_bit;
}


/* finds table that has the lowest gen_id greater than @prev->gen_id. returns
 * NULL when @prev is @ht's main table.
 */
static struct lfht_table *next_table_gen(
	const struct lfht *ht, const struct lfht_table *prev, bool filter_halted)
{
	unsigned long prev_gen = prev->gen_id;
	struct lfht_table *t = NULL;

	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(&ht->tables, &it);
		cur != NULL;
		cur = nbsl_next(&ht->tables, &it))
	{
		struct lfht_table *cand = container_of(cur, struct lfht_table, link);
		if(cand->gen_id <= prev_gen) break;
		if(!filter_halted
			|| atomic_load(&cand->halt_gen_id) < get_main(ht)->gen_id)
		{
			t = cand;
		}
	}

	return t;
}


void *lfht_firstval(const struct lfht *ht, struct lfht_iter *it, size_t hash)
{
	assert(e_inside());

	struct lfht_table *tab = get_main(ht);
	if(tab == NULL) return NULL;

	/* get the very last table. */
	struct nbsl_iter i;
	for(struct nbsl_node *cur = nbsl_first(&ht->tables, &i);
		cur != NULL;
		cur = nbsl_next(&ht->tables, &i))
	{
		tab = container_of(cur, struct lfht_table, link);
	}

	lfht_iter_init(it, tab, hash);
	for(;;) {
		void *val = ht_val(ht, it, hash);
		if(val != NULL) return val;
		else {
			/* next table plz */
			tab = next_table_gen(ht, it->t, false);
			if(tab == NULL) return NULL;
			lfht_iter_init(it, tab, hash);
		}
	}
}


void *lfht_nextval(const struct lfht *ht, struct lfht_iter *it, size_t hash)
{
	assert(e_inside());

	if(unlikely(it->t == NULL)) return NULL;

	/* next offset in same table. */
	it->perfect = 0;
	uintptr_t mask = (1ul << it->t->size_log2) - 1;
	it->off = (it->off + 1) & mask;

	void *ptr;
	if(it->off != it->end && (ptr = ht_val(ht, it, hash)) != NULL) {
		return ptr;
	}

	/* go to next table, etc. */
	do {
		struct lfht_table *tab = next_table_gen(ht, it->t, false);
		if(tab == NULL) return NULL;
		lfht_iter_init(it, tab, hash);
		ptr = ht_val(ht, it, hash);
	} while(ptr == NULL);

	return ptr;
}


void *lfht_first(const struct lfht *ht, struct lfht_iter *it)
{
	assert(e_inside());
	return NULL;
}


void *lfht_next(const struct lfht *ht, struct lfht_iter *it)
{
	assert(e_inside());
	return NULL;
}


void *lfht_prev(const struct lfht *ht, struct lfht_iter *it)
{
	assert(e_inside());
	return NULL;
}
