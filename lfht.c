
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <assert.h>

#include <ccan/likely/likely.h>
#include <ccan/container_of/container_of.h>

#include "lfht.h"
#include "epoch.h"


#define LFHT_DELETED (uintptr_t)1
#define LFHT_NA_FULL (~(uintptr_t)0)
#define LFHT_NA_EMPTY (LFHT_NA_FULL & ~(uintptr_t)1)

#define MIN_SIZE_LOG2 LFHT_MIN_TABLE_SIZE

#define POPCOUNT(x) __builtin_popcount((x))


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
	/* FIXME: make a function in nbsl.h for this */
	struct nbsl_node *n = (struct nbsl_node *)(tab->link.next & ~(uintptr_t)3);
	return container_of_or_null(n, struct lfht_table, link);
}


static uintptr_t get_perfect_bit(uintptr_t mask)
{
	/* deviate from CCAN htable by preferring very high-order bits. */
	for(int i = sizeof(uintptr_t) * 8 - 1; i >= 0; i--) {
		uintptr_t try = (uintptr_t)1 << i;
		if((mask & try) != 0) return try;
	}
	return 0;
}


static void set_bits(
	struct lfht_table *tab, const struct lfht_table *prev, void *model)
{
	if(prev == NULL) {
		/* punch MIN_SIZE_LOG2 - 1 bits' worth of holes in the common mask
		 * above typical malloc grain of 32 bytes.
		 */
		tab->common_mask = (~(uintptr_t)0 << (MIN_SIZE_LOG2 + 4)) | 0x1f;
		tab->perfect_bit = get_perfect_bit(tab->common_mask);
		assert(model != NULL);
		tab->common_bits = (uintptr_t)model & tab->common_mask;
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
			assert((tab->common_bits & ~tab->common_mask) == 0);
			if((tab->perfect_bit & tab->common_mask) == 0) {
				tab->perfect_bit = get_perfect_bit(tab->common_mask);
			}
		}
	}
	assert(model == NULL
		|| ((uintptr_t)model & tab->common_mask) == tab->common_bits);
	assert(tab->perfect_bit == 0
		|| (tab->perfect_bit & tab->common_mask) != 0);
	assert(POPCOUNT(tab->perfect_bit) <= 1);
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
	tab->elems = 0; tab->deleted = 0;
	tab->mig_left = 1L << sizelog2;
	tab->mig_next = (1L << sizelog2) - 1;
	tab->size_log2 = sizelog2;
	tab->gen_id = 0;
	tab->table = calloc(1L << sizelog2, sizeof(uintptr_t));
	if(tab->table == NULL) {
		free(tab);
		return NULL;
	}

	/* from CCAN htable */
	tab->max = ((size_t)3 << sizelog2) / 4;
	tab->max_with_deleted = ((size_t)9 << sizelog2) / 10;

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
		set_bits(nt, tab, model);
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
		set_bits(nt, tab, model);
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
	set_bits(nt, tab, NULL);
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
	assert(tab->elems == 0);	/* should be stable by now. */
	free(tab->table);
	free(tab);
}


static void remove_table(struct lfht *ht, struct lfht_table *tab)
{
	assert(tab != NULL);
	assert(get_next(tab) == NULL);

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


static bool ht_add(struct lfht_table *tab, const void *p, size_t hash)
{
	assert(((uintptr_t)p & tab->common_mask) == tab->common_bits);
	assert(POPCOUNT(tab->perfect_bit) <= 1);
	assert((tab->common_bits & tab->perfect_bit) == 0);
	uintptr_t perfect = tab->perfect_bit;
	size_t mask = (1ul << tab->size_log2) - 1, start = hash & mask;
	size_t i = start;
	do {
		uintptr_t e = atomic_load_explicit(&tab->table[i],
			memory_order_relaxed);
		if(entry_is_valid(e)) {
			if(!entry_is_avail(e)) {
				/* optimization: not-avail means @tab is secondary, so
				 * ht_add() should be tried again on the primary. this avoids
				 * an off-cpu migration, so it's worth it.
				 */
				return false;
			}
		} else {
			uintptr_t hval;

retry:
			hval = make_hval(tab, p, get_hash_ptr_bits(tab, hash) | perfect);
			if(e == LFHT_DELETED) {
				atomic_fetch_sub_explicit(&tab->deleted, 1,
					memory_order_relaxed);
			}
			uintptr_t old_e = e;
			if(atomic_compare_exchange_strong_explicit(&tab->table[i], &e, hval,
				memory_order_release, memory_order_relaxed))
			{
				return true;
			} else if(!entry_is_valid(e)) {
				/* exotic case: an empty slot was filled, then deleted. try
				 * again but fancily.
				 */
				assert(old_e != LFHT_DELETED);
				goto retry;
			} else {
				/* slot was snatched. undo and keep going. */
				if(old_e == LFHT_DELETED) atomic_fetch_add(&tab->deleted, 1);
			}
		}
		i = (i + 1) & mask;
		perfect = 0;
	} while(i != start);
	return false;
}


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
	} while(it->off != (hash & mask));

	return NULL;
}


/* check an entry in @src, migrate it to @dst (or @ht's main table) if valid.
 * returns true when @src became empty, or was already.
 */
static bool ht_migrate_entry(
	struct lfht *ht, struct lfht_table *dst, struct lfht_table *src)
{
	ssize_t spos = atomic_fetch_sub_explicit(&src->mig_next, 1,
		memory_order_consume);
	if(spos < 0) return true;

	uintptr_t e = atomic_load_explicit(&src->table[spos], memory_order_relaxed);
e_retry:
	if(!entry_is_valid(e)) {
		uintptr_t new = e == 0 ? LFHT_NA_EMPTY : LFHT_NA_FULL;
		if(!atomic_compare_exchange_strong_explicit(&src->table[spos],
			&e, new, memory_order_release, memory_order_relaxed))
		{
			/* a concurrent ht_add() filled it in. try again. */
			assert(entry_is_valid(e));
			assert(entry_is_avail(e));
			goto e_retry;
		}
	} else {
		size_t elems;

dst_retry:
		elems = atomic_fetch_add_explicit(&dst->elems, 1,
			memory_order_relaxed);
		assert(elems + 1 <= (1ul << dst->size_log2));
		void *ptr = get_raw_ptr(src, e);
		size_t hash = (*ht->rehash_fn)(ptr, ht->priv);
		/* !ok implies concurrent migration from @dst. */
		bool ok = ht_add(dst, ptr, hash);
		if(ok && atomic_compare_exchange_strong_explicit(&src->table[spos],
			&e, LFHT_NA_FULL, memory_order_relaxed, memory_order_relaxed))
		{
			atomic_fetch_sub_explicit(&src->elems, 1, memory_order_release);
		} else if(ok) {
			/* deleted under our feet. that's fine. */
			assert(!entry_is_valid(e));
			assert(entry_is_avail(e));
			/* drop the extra item from wherever it wound up at. */
			ok = lfht_del(ht, hash, ptr);
			assert(ok);
			goto e_retry;
		} else {
			/* @dst was made secondary. refetch and try again. */
			atomic_fetch_sub_explicit(&dst->elems, 1, memory_order_relaxed);
			struct lfht_table *rarest = get_main(ht);
			assert(rarest != dst);
			dst = rarest;
			goto dst_retry;
		}
	}

	if(atomic_fetch_sub_explicit(&src->mig_left, 1,
		memory_order_relaxed) == 1)
	{
		/* migration has emptied the table. it can now be removed. */
		assert(src->mig_next <= 0);
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
	struct lfht_table *sec = get_next(dst);
	if(sec == NULL) return;		/* nothing to do! */

	bool single = true;
	for(;;) {
		struct lfht_table *next = get_next(sec);
		if(next == NULL) break;
		single = false;
		sec = next;
	}

	int n_times = dst->size_log2 > sec->size_log2 && single ? 1 : 3;
	for(int i=0; i < n_times; i++) {
		if(ht_migrate_entry(ht, dst, sec)) break;
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
		tab = new_table(MIN_SIZE_LOG2);
		if(tab == NULL) goto fail;
		set_bits(tab, NULL, p);
		if(!nbsl_push(&ht->tables, NULL, &tab->link)) {
			free(tab->table);
			free(tab);
			tab = get_main(ht);
		}
	}

	/* ensure @tab has room for the new item, and won't fill up concurrently.
	 * if it would've filled up, add the next size of table. this is
	 * non-terminating under pathological circumstances, where room for the
	 * new element is instantly consumed by concurrent access.
	 */
	size_t elems;

retry:
	elems = atomic_fetch_add_explicit(&tab->elems, 1, memory_order_relaxed);
	if(((uintptr_t)p & tab->common_mask) != tab->common_bits) {
		atomic_fetch_sub_explicit(&tab->elems, 1, memory_order_relaxed);
		tab = remask_table(ht, tab, p);
		if(tab == NULL) goto fail;
		goto retry;
	} else if(elems + 1 > tab->max) {
		atomic_fetch_sub_explicit(&tab->elems, 1, memory_order_relaxed);
		tab = double_table(ht, tab, p);
		if(tab == NULL) goto fail;
		goto retry;
	} else if(elems + 1 + tab->deleted > tab->max_with_deleted) {
		struct lfht_table *oldtab = tab;
		tab = rehash_table(ht, tab);
		if(likely(tab != oldtab)) {
			atomic_fetch_sub_explicit(&oldtab->elems, 1, memory_order_relaxed);
			goto retry;
		}
	}

	if(!ht_add(tab, p, hash)) {
		/* tab was made secondary and migration twilight reached where @hash
		 * would land. undo and retry to avoid a further off-cpu migration.
		 */
		atomic_fetch_sub_explicit(&tab->elems, 1, memory_order_relaxed);
		tab = get_main(ht);
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
		atomic_fetch_add_explicit(&it->t->deleted, 1, memory_order_relaxed);
		atomic_fetch_sub_explicit(&it->t->elems, 1, memory_order_relaxed);
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
	it->t = tab;
	it->start = hash & ((1ul << it->t->size_log2) - 1);
	it->perfect = tab->perfect_bit;
	it->off = it->start;
}


/* finds table that has the lowest gen_id greater than @prev->gen_id. returns
 * NULL when @prev is @ht's main table.
 */
static struct lfht_table *next_table_gen(
	const struct lfht *ht, const struct lfht_table *prev)
{
	unsigned long prev_gen = prev->gen_id;
	struct lfht_table *t = NULL;

	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(&ht->tables, &it);
		cur != NULL;
		cur = nbsl_next(&ht->tables, &it))
	{
		if(cur == &prev->link) break;
		struct lfht_table *cand = container_of(cur, struct lfht_table, link);
		if(cand->gen_id <= prev_gen) break;
		t = cand;
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
			tab = next_table_gen(ht, it->t);
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
	if(it->off != it->start && (ptr = ht_val(ht, it, hash)) != NULL) {
		return ptr;
	}

	/* go to next table, etc. */
	do {
		struct lfht_table *tab = next_table_gen(ht, it->t);
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
