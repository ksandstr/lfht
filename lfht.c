
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <assert.h>
#include <sched.h>

#include <ccan/likely/likely.h>
#include <ccan/container_of/container_of.h>

#include "lfht.h"
#include "epoch.h"


#define LFHT_DELETED (uintptr_t)1
#define LFHT_NA_FULL (~(uintptr_t)0)
#define LFHT_NA_EMPTY (LFHT_NA_FULL & ~(uintptr_t)1)

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


static inline bool entry_is_valid(uintptr_t e) {
	return e > LFHT_DELETED;
}


static inline bool entry_is_avail(uintptr_t e) {
	return e != LFHT_NA_FULL && e != LFHT_NA_EMPTY;
}


static inline struct lfht_table *get_main(const struct lfht *lfht) {
	return container_of_or_null(nbsl_top(&lfht->tables),
		struct lfht_table, link);
}


static inline struct lfht_table *get_next(const struct lfht_table *tab) {
	return nbsl_next_node(&tab->link, struct lfht_table, link);
}


static inline uintptr_t get_perfect_bit(const struct lfht_table *tab)
{
	/* deviate from CCAN htable by preferring very high-order bits. could
	 * replace MSB(...) with ffsl(...) - 1 to do the opposite, but why bother?
	 */
	return tab->common_mask == 0 ? 0 : (uintptr_t)1 << MSB(tab->common_mask);
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
		tab->perfect_bit = get_perfect_bit(tab);
	} else {
		tab->common_mask = prev->common_mask;
		tab->common_bits = prev->common_bits;
		assert(POPCOUNT(prev->perfect_bit) <= 1);
		tab->perfect_bit = prev->perfect_bit;

		uintptr_t m = (uintptr_t)model;
		if(model != NULL && (m & tab->common_mask) != tab->common_bits) {
			uintptr_t new = tab->common_bits ^ (m & tab->common_mask);
			assert((new & tab->common_mask) != 0);
			tab->common_mask &= ~new;
			tab->common_bits &= ~new;
			assert((m & tab->common_mask) == tab->common_bits);
			tab->perfect_bit = get_perfect_bit(tab);
		}
	}

	assert((tab->common_bits & ~tab->common_mask) == 0);
	assert(model == NULL
		|| ((uintptr_t)model & tab->common_mask) == tab->common_bits);

	assert(POPCOUNT(tab->perfect_bit) <= 1);
	assert(tab->perfect_bit == 0
		|| (tab->perfect_bit & tab->common_mask) != 0);
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

	/* set max probe depth to max(16, n_entries / 32), i.e. as low as two
	 * cachelines on 64-bit which'll touch 3 cachelines on average. this
	 * causes mildly pessimal performance (and heavy reliance on the runtime's
	 * lazy heap) when a single hash chain is very long, or the hash function
	 * generates a poor distribution from e.g. strings that share a prefix,
	 * such as multiply-accumulate variations; but then recovers as the table
	 * gets bigger.
	 */
	tab->max_probe = (1ul << sizelog2) / 32;
	if(tab->max_probe < MIN_PROBE) tab->max_probe = MIN_PROBE;

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


/* install a new table of exactly the same size. ht_add() will migrate two
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
	assert(get_total_elems(tab) == 0);	/* should be stable by now. */
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
	assert(entry_is_valid((uintptr_t)p));
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
		& tab->common_mask & ~tab->perfect_bit;
}


static inline uintptr_t get_extra_ptr_bits(
	const struct lfht_table *tab, uintptr_t e)
{
	return e & tab->common_mask;
}


static inline void *get_raw_ptr(const struct lfht_table *tab, uintptr_t e) {
	return (void *)((e & ~tab->common_mask) | tab->common_bits);
}


/* returns 0 on success, 1 to indicate that the caller should reload @tab, and
 * -1 when probe distance was exceeded.
 */
static int ht_add(struct lfht_table *tab, const void *p, size_t hash)
{
	assert(((uintptr_t)p & tab->common_mask) == tab->common_bits);
	assert(POPCOUNT(tab->perfect_bit) <= 1);
	uintptr_t perfect = tab->perfect_bit;
	size_t mask = (1ul << tab->size_log2) - 1, start = hash & mask,
		end = (start + tab->max_probe) & mask,
		i = start;
	do {
		uintptr_t e = atomic_load_explicit(&tab->table[i],
			memory_order_relaxed);
		if(entry_is_valid(e)) {
			if(!entry_is_avail(e)) {
				/* optimization: not-avail means @tab is secondary, so
				 * ht_add() should be tried again on the primary. this avoids
				 * an off-cpu migration, so it's worth it.
				 */
				return 1;
			}
		} else {
			uintptr_t hval;

retry:
			hval = make_hval(tab, p, get_hash_ptr_bits(tab, hash) | perfect);
			if(e == LFHT_DELETED) {
				atomic_fetch_sub_explicit(&DELETED(tab), 1,
					memory_order_relaxed);
			}
			uintptr_t old_e = e;
			if(atomic_compare_exchange_strong_explicit(&tab->table[i], &e, hval,
				memory_order_release, memory_order_relaxed))
			{
				return 0;
			} else if(!entry_is_valid(e)) {
				/* exotic case: an empty slot was filled, then deleted. try
				 * again but fancily.
				 */
				assert(old_e != LFHT_DELETED);
				goto retry;
			} else {
				/* slot was snatched. undo and keep going. */
				if(old_e == LFHT_DELETED) atomic_fetch_add(&DELETED(tab), 1);
			}
		}
		i = (i + 1) & mask;
		perfect = 0;
	} while(i != end);
	return -1;
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
		if(e == 0 || e == LFHT_NA_EMPTY) break;
		if(e != LFHT_DELETED && e != LFHT_NA_FULL) {
			if(get_extra_ptr_bits(it->t, e) == h2) {
				return get_raw_ptr(it->t, e);
			}
		}
		it->off = (it->off + 1) & mask;
		h2 &= ~perfect;
	} while(it->off != it->end);

	return NULL;
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


/* check an entry in @src, migrate it to @dst (or @ht's main table) if valid.
 * returns true when @src became empty, was already empty, or migration was
 * blocked on it.
 */
static bool ht_migrate_entry(
	struct lfht *ht, struct lfht_table *dst, struct lfht_table *src)
{
	assert(src != dst);
	assert(POPCOUNT(dst->common_mask) <= POPCOUNT(src->common_mask));
	assert(src->gen_id < dst->gen_id);

	struct lfht_table_percpu *src_pc;
	ssize_t spos;
	bool last_chunk;
spos_retry:
	spos = take_mig_work(&last_chunk, &src_pc, src);
	if(spos < 0) return true;

	uintptr_t e = atomic_load_explicit(&src->table[spos],
		memory_order_relaxed);
e_retry:
	if(!entry_is_avail(e)) {
		/* in a table where migration was previously halted, non-available
		 * rows may be encountered. they should be skipped in a loop.
		 */
		assert(src->halt_gen_id > 0);
		goto spos_retry;
	}
	if(!entry_is_valid(e)) {
		uintptr_t new = e == 0 ? LFHT_NA_EMPTY : LFHT_NA_FULL;
		if(!atomic_compare_exchange_strong_explicit(&src->table[spos],
			&e, new, memory_order_release, memory_order_relaxed))
		{
			/* concurrent modification: a ht_add() filled the slot in, a
			 * lfht_delval() deleted the item, or a ht_migrate_entry() moved
			 * the entry out (implying that migration was halted on @src).
			 */
			if(src->halt_gen_id > 0 && !entry_is_avail(e)) goto spos_retry;
			else {
				assert(entry_is_valid(e) || e == LFHT_DELETED);
				goto e_retry;
			}
		}
	} else {
		struct lfht_table_percpu *dst_pc;
dst_retry:
		dst_pc = MY_PERCPU(dst);
		atomic_fetch_add_explicit(&dst_pc->elems, 1, memory_order_relaxed);
		void *ptr = get_raw_ptr(src, e);
		size_t hash = (*ht->rehash_fn)(ptr, ht->priv);
		int n = ht_add(dst, ptr, hash);
		if(n == 0 && atomic_compare_exchange_strong_explicit(&src->table[spos],
			&e, LFHT_NA_FULL, memory_order_relaxed, memory_order_relaxed))
		{
			atomic_fetch_sub_explicit(&src_pc->elems, 1, memory_order_release);
		} else if(n == 0) {
			/* deleted under our feet (or migrated, but that only happens if
			 * migration from @src was previously halted). that's fine.
			 */
			assert(!entry_is_valid(e) || src->halt_gen_id > 0);
			assert(entry_is_avail(e) || src->halt_gen_id > 0);
			/* drop the extra item from wherever it wound up at. */
			bool ok = lfht_del(ht, hash, ptr);
			assert(ok);
			goto e_retry;
		} else if(n > 0) {
			/* @dst was made secondary. refetch and try again. */
			atomic_fetch_sub_explicit(&dst_pc->elems, 1, memory_order_relaxed);
			struct lfht_table *rarest = get_main(ht);
			assert(rarest != dst);
			dst = rarest;
			goto dst_retry;
		} else {
			assert(n < 0);
			/* ptr/hash can't be inserted because probe length was exceeded.
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
			atomic_fetch_sub_explicit(&dst_pc->elems, 1, memory_order_relaxed);
			increase_to(&src->halt_gen_id, dst->gen_id);
			increase_to(&src_pc->mig_next, spos);
			/* skip this table; migration from somewhere else may succeed. */
			return true;
		}
	}

	if(atomic_fetch_sub_explicit(&src_pc->mig_left, 1,
			memory_order_relaxed) == 1
		&& (last_chunk || get_total_mig_left(src) == 0))
	{
		/* migration has emptied the table. it can now be removed. */
		assert(src_pc->mig_next < src_pc->mig_last || src->halt_gen_id > 0);
		remove_table(ht, src);
		return true;
	} else {
		/* go on. */
		return false;
	}
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
		if(ht_migrate_entry(ht, dst, sec) && n_times > 1) {
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

	struct lfht_table_percpu *pc;

retry:
	pc = MY_PERCPU(tab);
	atomic_fetch_add_explicit(&pc->elems, 1, memory_order_relaxed);

	if(((uintptr_t)p & tab->common_mask) != tab->common_bits) {
		atomic_fetch_sub_explicit(&pc->elems, 1, memory_order_relaxed);
		tab = remask_table(ht, tab, p);
		if(tab == NULL) goto fail;
		goto retry;
	}

	int n = ht_add(tab, p, hash);
	if(n > 0) {
		/* tab was made secondary and migration twilight reached where @hash
		 * would land. undo and retry to avoid a further off-cpu migration.
		 */
		atomic_fetch_sub_explicit(&pc->elems, 1, memory_order_relaxed);
		tab = get_main(ht);
		goto retry;
	} else if(n < 0) {
		/* probe limit was reached. double or rehash the table. */
		size_t elems, deleted;
		get_totals(&elems, &deleted, NULL, tab);
		if(elems + 1 <= tab->max
			&& elems + 1 + deleted > tab->max_with_deleted)
		{
			struct lfht_table *oldtab = tab;
			tab = rehash_table(ht, tab);
			if(likely(tab != oldtab)) {
				atomic_fetch_sub_explicit(&ELEMS(oldtab), 1,
					memory_order_relaxed);
			}
		} else {
			atomic_fetch_sub_explicit(&pc->elems, 1, memory_order_relaxed);
			tab = double_table(ht, tab, p);
			if(tab == NULL) goto fail;
		}
		goto retry;
	}

	ht_migrate(ht, tab);

	e_end(eck);
	return true;

fail:
	e_end(eck);
	return false;
}


bool lfht_delval(const struct lfht *ht, struct lfht_iter *it, void *p)
{
	assert(e_inside());

	uintptr_t e = atomic_load_explicit(&it->t->table[it->off],
		memory_order_relaxed);
	if(get_raw_ptr(it->t, e) == p
		&& atomic_compare_exchange_strong_explicit(
			&it->t->table[it->off], &e, LFHT_DELETED,
			memory_order_release, memory_order_relaxed))
	{
		struct lfht_table_percpu *pc = MY_PERCPU(it->t);
		atomic_fetch_add_explicit(&pc->deleted, 1, memory_order_relaxed);
		atomic_fetch_sub_explicit(&pc->elems, 1, memory_order_relaxed);
		return true;
	} else {
		return false;
	}
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
