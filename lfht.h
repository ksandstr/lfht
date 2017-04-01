
#ifndef LFHT_H
#define LFHT_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "nbsl.h"


#define LFHT_MIN_TABLE_SIZE 5	/* 32 entries = 2 cachelines on LP64 */

#define CACHELINE_ALIGN __attribute__((aligned(64)))


struct lfht_table
{
	struct nbsl_node link;	/* in <struct lfht>.tables */

	/* consistent at `link' or `table' release */
	size_t elems, deleted;
	/* monotonically decreasing */
	_Atomic ssize_t mig_next, mig_left;

	/* constants */
	uintptr_t *table CACHELINE_ALIGN;	/* allocated separately */
	/* common_mask indicates bits that're the same across all keys;
	 * common_bits specifies what those bits are. perfect_bit, when nonzero,
	 * is always set in common_mask, and cleared in common_bits.
	 */
	uintptr_t common_mask, common_bits, perfect_bit;
	unsigned long gen_id;		/* next == NULL || gen_id > next->gen_id */
	size_t max, max_with_deleted, max_probe;
	unsigned int size_log2;		/* 1 << size_log2 < SSIZE_MAX */
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

extern bool lfht_add(struct lfht *ht, size_t hash, void *p);
extern bool lfht_del(struct lfht *ht, size_t hash, const void *p);

/* valid for lfht_del_at() iff ->off != ->end, or rather,
 * ->off < ->end (mod @t->size).
 */
struct lfht_iter {
	struct lfht_table *t;
	size_t off, end;
	uintptr_t perfect;
};

extern void *lfht_firstval(
	const struct lfht *ht, struct lfht_iter *it, size_t hash);

extern void *lfht_nextval(
	const struct lfht *ht, struct lfht_iter *it, size_t hash);

/* convenience function for retrieving the first matching item. caller must
 * have an existing epoch bracket, or the returned pointer will be invalid and
 * the iteration will go into undefined la-la land.
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
