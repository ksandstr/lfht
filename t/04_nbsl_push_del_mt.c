
/* multithreaded push20/del15 test on the non-blocking single-link list. */

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


#define TEST_SIZE 1000
#define N_THREADS 16


struct item {
	struct nbsl_node link;
	size_t value;
};


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


static void *add_and_remove_items_fn(void *param_ptr)
{
	struct nbsl *list = param_ptr;
	pthread_barrier_wait(&launch_barrier);
	const int thread_id = atomic_fetch_add(&next_thread_id, 1);

	darray(struct item *) *ret = malloc(sizeof(*ret));
	darray_init(*ret);

	bool first = true;
	for(int i=0; i < TEST_SIZE; i++) {
		struct item *them[20];
		for(int j=0; j < 20; j++) {
			them[j] = push(list, thread_id * TEST_SIZE * 20 + 20 * i + j);
		}
		for(int j=0; j < 15; j++) {
			bool ok = nbsl_del(list, &them[j + 2]->link);
			if(!ok && first) {
				first = false;
				diag("thread_id=%d failed to remove item=%zu", thread_id,
					them[j + 2]->value);
			}
			if(ok) darray_push(*ret, them[j + 2]);
		}
	}
	diag("thread_id=%d completed", thread_id);

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

	plan_tests(2);

	/* make an empty list. */
	struct nbsl *list = malloc(sizeof(*list));
	nbsl_init(list);

	/* have some threads bang it heavily. */
	pthread_barrier_init(&launch_barrier, NULL, N_THREADS);
	pthread_t oth[N_THREADS];
	for(size_t i=0; i < N_THREADS; i++) {
		int n = pthread_create(&oth[i], NULL, &add_and_remove_items_fn, list);
		if(n != 0) {
			diag("pthread_create: n=%d", n);
			abort();
		}
	}

	/* get results. */
	darray(struct item *) *result[N_THREADS];
	for(size_t i=0; i < N_THREADS; i++) {
		void *ret = NULL;
		int n = pthread_join(oth[i], &ret);
		if(n != 0) {
			diag("pthread_join: i=%zu, n=%d", i, n);
			abort();
		}
		result[i] = ret;
		if(TEST_SIZE / N_THREADS > 200 && result[i]->size < 100) {
			diag("result[%zu]->size=%zu", i, result[i]->size);
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

	size_t total = remain.size;
	for(size_t i=0; i < N_THREADS; i++) total += result[i]->size;
	if(!ok1(total == n_items)) {
		diag("total=%zu, expected %zu", total, n_items);
	}

	/* each item should appear exactly once. */
	darray(struct item *) all = darray_new();
	darray_append_items(all, remain.item, remain.size);
	for(size_t i=0; i < N_THREADS; i++) {
		darray_append_items(all, result[i]->item, result[i]->size);
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
