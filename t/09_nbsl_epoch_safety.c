
/* tests on epoch-compatible removal (safety) under multithreaded
 * push20/del15/del5i load. disproves safety by crashing.
 */

#include <stdlib.h>
#include <stdatomic.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>
#include <ccan/container_of/container_of.h>
#include <ccan/darray/darray.h>

#include "nbsl.h"
#include "epoch.h"


#define TEST_SIZE 200
#define N_THREADS 16


struct item {
	struct nbsl_node link;
	size_t value;
};


typedef darray(struct item *) item_array;


static pthread_barrier_t launch_barrier;
static _Atomic int next_thread_id = 0;


/* provokes breakage when @item->link is used after epoch. */
static void overwrite_fn(struct item *item) {
	memset(item, 0xff, sizeof(*item));
}


static struct item *push(struct nbsl *list, size_t value)
{
	struct item *it = malloc(sizeof(*it));
	it->value = value;
	while(!nbsl_push(list, nbsl_top(list), &it->link)) {
		/* spin */
	}
	return it;
}


static struct item *pop(struct nbsl *list)
{
	struct nbsl_node *link = nbsl_pop(list);
	return container_of_or_null(link, struct item, link);
}


static int cmp_raw_ptr(const void *a, const void *b) {
	const void *const *aa = a, *const *bb = b;
	return (intptr_t)*aa - (intptr_t)*bb;
}


/* $(RETVAL & (1 << n)) != 0 iff @them[n] ∈ @list$. */
static size_t presence_mask_and_del(
	item_array *deleted_items,
	struct nbsl *list, struct item **them, size_t n_them, size_t del_mask)
{
	assert(n_them <= sizeof(size_t) * 8);
	size_t found = 0, full = (1u << n_them) - 1;
	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(list, &it);
		cur != NULL && found != full;
		cur = nbsl_next(list, &it))
	{
		struct item **p = bsearch(&cur, them, n_them,
			sizeof(struct item *), &cmp_raw_ptr);
		if(p == NULL) continue;

		int ix = p - them;
		assert(ix >= 0 && ix < n_them);
		assert(them[ix] == container_of(cur, struct item, link));
		assert((found & ((size_t)1 << ix)) == 0);
		found |= (size_t)1 << ix;

		if((del_mask & ((size_t)1 << ix)) != 0) {
			bool ok = nbsl_del_at(list, &it);
			if(ok && deleted_items != NULL) {
				struct item *it = container_of(cur, struct item, link);
				darray_push(*deleted_items, it);
				e_call_dtor(&overwrite_fn, it);
			}
		}
	}

	return found;
}


struct result {
	item_array items;
	bool all_found, all_lost;
};


static void *p20_d15i_thread_fn(void *param_ptr)
{
	struct nbsl *list = param_ptr;
	pthread_barrier_wait(&launch_barrier);
	const int thread_id = atomic_fetch_add(&next_thread_id, 1);

	struct result *ret = malloc(sizeof(*ret));
	darray_init(ret->items);
	ret->all_found = true;	/* non-deleted items are seen */
	ret->all_lost = true;	/* deleted items aren't seen */

	int eck = e_begin();
	bool first = true;
	for(int i=0; i < TEST_SIZE; i++) {
		struct item *them[20];
		for(int j=0; j < 20; j++) {
			them[j] = push(list, thread_id * TEST_SIZE * 20 + 20 * i + j);
		}
		/* allow bsearch */
		qsort(them, 20, sizeof(struct item *), &cmp_raw_ptr);

		/* check that they appear, but be a bit clever about it. */
		unsigned found = presence_mask_and_del(NULL, list, them, 20, 0);
		if(found != 0xfffff && ret->all_found) {
			diag("thread_id=%d, i=%d, found=%#x (not all)",
				thread_id, i, found);
			ret->all_found = false;
		}

		/* delete 15. */
		for(int j=0; j < 15; j++) {
			bool ok = nbsl_del(list, &them[j + 2]->link);
			if(!ok && first) {
				first = false;
				diag("thread_id=%d failed to remove item=%zu", thread_id,
					them[j + 2]->value);
			}
			if(ok) {
				darray_push(ret->items, them[j + 2]);
				e_call_dtor(&overwrite_fn, them[j + 2]);
			}
		}

		/* delete the rest with nbsl_del_at(). */
		found = presence_mask_and_del(&ret->items, list, them, 20, 0xe0003);
		if(found != 0xe0003 && ret->all_lost) {
			diag("thread_id=%d, i=%d, found=%#x (wrong pattern)",
				thread_id, i, found);
			ret->all_lost = false;
		}

		if((i % 123) == 0) {
			e_end(eck);
			eck = e_begin();
		}
	}
	e_end(eck);

	return ret;
}


int main(void)
{
	const size_t n_items = N_THREADS * TEST_SIZE * 20;

	plan_tests(4);

	/* make an empty list. */
	struct nbsl *list = malloc(sizeof(*list));
	nbsl_init(list);

	/* have some threads bang it heavily. */
	pthread_barrier_init(&launch_barrier, NULL, N_THREADS);
	pthread_t oth[N_THREADS];
	for(size_t i=0; i < N_THREADS; i++) {
		int n = pthread_create(&oth[i], NULL, &p20_d15i_thread_fn, list);
		if(n != 0) {
			diag("pthread_create: n=%d", n);
			abort();
		}
	}

	/* get results. */
	struct result *rs[N_THREADS];
	for(size_t i=0; i < N_THREADS; i++) {
		void *ret = NULL;
		int n = pthread_join(oth[i], &ret);
		if(n != 0) {
			diag("pthread_join: i=%zu, n=%d", i, n);
			abort();
		}
		rs[i] = ret;
		if(TEST_SIZE / N_THREADS > 200 && rs[i]->items.size < 100) {
			diag("rs[%zu]->items.size=%zu", i, rs[i]->items.size);
		}
	}
	pass("didn't crash despite epoch-dtor overwrite");

	/* analyze 'em. */
	darray(struct item *) remain = darray_new();
	for(;;) {
		struct item *it = pop(list);
		if(it == NULL) break;
		darray_push(remain, it);
	}
	diag("remain.size=%zu", remain.size);

	/* all_found and all_lost should be true for all of them */
	bool all_found = true, all_lost = true;
	for(int i=0; i < N_THREADS; i++) {
		all_found = all_found & rs[i]->all_found;
		all_lost = all_lost & rs[i]->all_lost;
	}
	ok1(all_found);
	ok1(all_lost);

	/* totals should add up */
	size_t total = remain.size;
	for(size_t i=0; i < N_THREADS; i++) total += rs[i]->items.size;
	if(!ok1(total == n_items)) {
		diag("total=%zu, expected %zu", total, n_items);
	}

	return exit_status();
}
