
#ifndef LFHT_H
#define LFHT_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "nbsl.h"
#include "percpu.h"


#define LFHT_MIN_TABLE_SIZE 5	/* 32 entries = 2 cachelines on LP64 */

#define CACHELINE_ALIGN __attribute__((aligned(64)))


struct lfht_table
{
	struct nbsl_node link;	/* in <struct lfht>.tables */

	/* increase-only via cmpxchg.
	 * halt migration if halt_gen_id >= main table's gen_id.
	 */
	_Atomic unsigned long halt_gen_id;

	/* constants */
	uintptr_t *table CACHELINE_ALIGN;	/* allocated separately */
	struct percpu *pc;			/* of <struct lfht_table_percpu> */
	/* common_mask indicates bits that're the same across all keys;
	 * common_bits specifies what those bits are.
	 *
	 * *_bit are the values of reserved bits. they are either set in
	 * common_mask and resv_mask or (in the case of perfect_bit) unallocated
	 * and equal to zero. reserved bits' positions are never used to store
	 * match bits derived from the hash value.
	 */
	uintptr_t common_mask, common_bits, resv_mask;
	uintptr_t perfect_bit;

	/* migration state bits. the interesting part.
	 *
	 * src_bit is set in a migration source entry until the state of the
	 * source half is resolved to either successful migration or concurrent
	 * deletion. it may co-occur with ephem_bit due to a delayed migration; in
	 * this case migration pointer chaining occurs.
	 *
	 * del_bit marks late deletion in a migration source or ephemeral target
	 * entry when concurrent deletion finds that src_bit was set, or ephem_bit
	 * was set and a corresponding migration pointer was found.
	 *
	 * ephem_bit is set in a migration target entry before the migration is
	 * confirmed successful. it's mutually exclusive wrt hazard_bit.
	 *
	 * mig_bit is set in entries that've been migrated. it designates an
	 * independent subformat where mig_bit is set and other bits are used for
	 * a 10-bit gen_id offset and the other 21/53 for a probe address, or
	 * post-migration tombstone values when gen_id=0. the low-level details
	 * are documented in lfht.c .
	 *
	 * hazard_bit is set in slots that're subject to a potential ABBA race if
	 * the entry were deleted, and then the same value migrated in again.
	 * ht_add() will skip slots with hazard_bit set when the incoming entry
	 * has ephem_bit set, and migration will clear hazard_bit once it has
	 * cleared the corresponding migration pointer.
	 */
	uintptr_t ephem_bit, src_bit, del_bit, mig_bit, hazard_bit;

	unsigned long gen_id;		/* next == NULL || gen_id > next->gen_id */
	size_t max, max_with_deleted, max_probe;
	unsigned short size_log2;	/* 1 << size_log2 < SSIZE_MAX */
	unsigned short probe_addr_size_log2;
};


struct lfht_table_percpu
{
	size_t elems, deleted;	/* split-sum counters */

	/* things that're not regularly read from off-CPU; they can go on their
	 * own cache line.
	 */
	_Atomic int total_check_count __attribute__((aligned(64)));

	/* monotonically decreasing.
	 * mig_next can increase iff halt_gen_id > 0.
	 */
	_Atomic ssize_t mig_next, mig_left;
	ssize_t mig_last;
};


struct lfht
{
	struct nbsl tables;

	/* constants */
	size_t (*rehash_fn)(const void *ptr, void *priv);
	void *priv;
	unsigned int first_size_log2;	/* size of first table */
};


#define LFHT_INITIALIZER(name, rehash, priv) \
	{ NBSL_LIST_INIT(name.tables), (rehash), (priv), LFHT_MIN_TABLE_SIZE }


extern void lfht_init(
	struct lfht *ht,
	size_t (*rehash_fn)(const void *ptr, void *priv), void *priv);

extern void lfht_init_sized(
	struct lfht *ht,
	size_t (*rehash_fn)(const void *ptr, void *priv), void *priv,
	size_t size);

extern void lfht_clear(struct lfht *ht);
/* TODO: lfht_copy(), lfht_rehash() */


/* valid for lfht_del_at() iff ->off != ->end, or rather,
 * ->off < ->end (mod @t->size).
 */
struct lfht_iter {
	struct lfht_table *t;
	size_t off, end, hash;
	uintptr_t perfect;
};

#define LFHT_ADD_ITER(hash_) ((struct lfht_iter){ .hash = (hash_) })

extern void *lfht_firstval(
	const struct lfht *ht, struct lfht_iter *it, size_t hash);

extern void *lfht_nextval(
	const struct lfht *ht, struct lfht_iter *it, size_t hash);

/* same as lfht_add(), but when the hash remains the same from call to
 * another, multiset insert becomes O(n) instead of O(n^2). @iter should point
 * to a lfht_iter initialized with LFHT_ADD_ITER() before first call, when the
 * hash changes, and after end of epoch.
 */
extern bool lfht_add_many(struct lfht *ht, struct lfht_iter *iter, void *p);


static inline bool lfht_add(struct lfht *ht, size_t hash, void *p) {
	struct lfht_iter it = LFHT_ADD_ITER(hash);
	return lfht_add_many(ht, &it, p);
}

extern bool lfht_del(struct lfht *ht, size_t hash, const void *p);

/* convenience function for retrieving the first matching item. caller must
 * have an existing epoch bracket, or the returned pointer will be invalid and
 * iteration will go into undefined la-la land.
 */
static inline void *lfht_get(
	const struct lfht *ht, size_t hash,
	bool (*cmp_fn)(const void *cand, void *ptr), const void *ptr)
{
	struct lfht_iter it;
	for(void *cand = lfht_firstval(ht, &it, hash);
		cand != NULL;
		cand = lfht_nextval(ht, &it, hash))
	{
		if((*cmp_fn)(cand, (void *)ptr)) return cand;
	}
	return NULL;
}

extern void *lfht_first(const struct lfht *ht, struct lfht_iter *it);
extern void *lfht_next(const struct lfht *ht, struct lfht_iter *it);
extern void *lfht_prev(const struct lfht *ht, struct lfht_iter *it);

/* returns true if @p was deleted, false otherwise. */
extern bool lfht_delval(const struct lfht *ht, struct lfht_iter *it, void *p);


#endif
