
#define _ISOC11_SOURCE

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "lfht.h"
#include "epoch.h"


#define MIN_SIZE_LOG2 5		/* 2 cachelines' worth */


void lfht_init(
	struct lfht *ht,
	size_t (*rehash_fn)(const void *ptr, void *priv), void *priv)
{
	struct lfht t = LFHT_INITIALIZER(t, rehash_fn, priv);
	*ht = t;
}


bool lfht_init_sized(
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
	struct lfht_table *tab = aligned_alloc(64, sizeof(*tab));
	if(tab == NULL) return false;
	tab->next = NULL;
	tab->elems = 0; tab->deleted = 0;
	tab->last_valid = (1L << sizelog2) - 1;
	tab->size_log2 = sizelog2;
	tab->table = calloc(1L << sizelog2, sizeof(uintptr_t));
	if(tab->table == NULL) {
		free(tab);
		return false;
	}
	/* from CCAN htable */
	tab->max = ((size_t)3 << sizelog2) / 4;
	tab->max_with_deleted = ((size_t)9 << sizelog2) / 10;

	atomic_store_explicit(&ht->main, tab, memory_order_release);
	return true;
}


void lfht_clear(struct lfht *ht)
{
	int eck = e_begin();
	struct lfht_table *_Atomic *pp = &ht->main;
	for(;;) {
		struct lfht_table *tab = atomic_load_explicit(pp,
			memory_order_relaxed);
		if(tab == NULL) break;	/* end */
		if(!atomic_compare_exchange_strong(pp, &tab, NULL)) {
			/* concurrent lfht_clear(); let it/them run */
			break;
		}
		pp = &tab->next;
		e_free(tab->table);
		e_free(tab);	/* NOTE: this doesn't invalidate *pp. */
	}
	e_end(eck);
}


bool lfht_add(struct lfht *ht, size_t hash, void *p)
{
	return false;
}


bool lfht_delval(const struct lfht *ht, struct lfht_iter *it, void *p)
{
	return false;
}


bool lfht_del(struct lfht *ht, size_t hash, const void *p)
{
	struct lfht_iter it;
	for(void *c = lfht_firstval(ht, &it, hash);
		c != NULL;
		c = lfht_nextval(ht, &it, hash))
	{
		if(c == p && lfht_delval(ht, &it, c)) return true;
	}
	return false;
}


void *lfht_firstval(const struct lfht *ht, struct lfht_iter *it, size_t hash)
{
	return NULL;
}


void *lfht_nextval(const struct lfht *ht, struct lfht_iter *it, size_t hash)
{
	return NULL;
}


void *lfht_first(const struct lfht *ht, struct lfht_iter *it)
{
	return NULL;
}


void *lfht_next(const struct lfht *ht, struct lfht_iter *it)
{
	return NULL;
}


void *lfht_prev(const struct lfht *ht, struct lfht_iter *it)
{
	return NULL;
}
