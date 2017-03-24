
/* tests on deletion via iterator under multithreaded push20/del15 load. */

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


#define TEST_SIZE 200
#define N_THREADS 16


struct item {
	struct nbsl_node link;
	size_t value;
};


typedef darray(struct item *) item_array;


static pthread_barrier_t launch_barrier;
static _Atomic int next_thread_id = 0;


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
				darray_push(*deleted_items,
					container_of(cur, struct item, link));
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
			if(ok) darray_push(ret->items, them[j + 2]);
		}

		/* delete the rest with nbsl_del_at(). */
		found = presence_mask_and_del(&ret->items, list, them, 20, 0xe0003);
		if(found != 0xe0003 && ret->all_lost) {
			diag("thread_id=%d, i=%d, found=%#x (wrong pattern)",
				thread_id, i, found);
			ret->all_lost = false;
		}
	}

	return ret;
}


static int item_ptr_cmp(const void *a, const void *b)
{
	const struct item *const *aa = a, *const *bb = b;
	return (ssize_t)(*aa)->value - (ssize_t)(*bb)->value;
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

	/* each item should appear exactly once. */
	darray(struct item *) all = darray_new();
	darray_append_items(all, remain.item, remain.size);
	for(size_t i=0; i < N_THREADS; i++) {
		darray_append_items(all, rs[i]->items.item, rs[i]->items.size);
	}
	assert(all.size == total);
	qsort(all.item, all.size, sizeof(struct item *), &item_ptr_cmp);
	bool uniq = true;
	for(size_t i=1; i < all.size; i++) {
		if(all.item[i - 1]->value == all.item[i]->value) {
			diag("item[%zu]->value=%zu, item[%zu]->value=%zu",
				i - 1, all.item[i - 1]->value, i, all.item[i]->value);
			uniq = false;
			break;
		}
	}
	ok(uniq && all.size == n_items,
		"each item was popped exactly once");

	return exit_status();
}
