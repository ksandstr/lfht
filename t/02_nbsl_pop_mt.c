
/* multithreaded pop test on the non-blocking single-link list. */

#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>
#include <ccan/container_of/container_of.h>
#include <ccan/darray/darray.h>

#include "nbsl.h"


struct item {
	struct nbsl_node link;
	size_t value;
};


static pthread_barrier_t launch_barrier;


static struct item *push(struct nbsl *list, size_t value)
{
	struct item *it = malloc(sizeof(*it));
	it->value = value;
	int spins = 0;
	while(!nbsl_push(list, nbsl_top(list), &it->link)) {
		/* spin */
		diag("%s: repeating", __func__);
		if(++spins == 10) abort();
	}
	return it;
}


static struct item *pop(struct nbsl *list)
{
	struct nbsl_node *link = nbsl_pop(list);
	return container_of_or_null(link, struct item, link);
}


static struct item *top(struct nbsl *list)
{
	struct nbsl_node *link = nbsl_top(list);
	return container_of_or_null(link, struct item, link);
}


static void *grab_items_fn(void *param_ptr)
{
	struct nbsl *list = param_ptr;
	pthread_barrier_wait(&launch_barrier);
	darray(struct item *) *ret = malloc(sizeof(*ret));
	darray_init(*ret);
	for(;;) {
		struct item *it = pop(list);
		if(it == NULL) break;
		darray_push(*ret, it);
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
	const size_t test_size = 200000, n_threads = 16;

	plan_tests(4);

	struct nbsl *list = malloc(sizeof(*list));
	nbsl_init(list);
	/* build a big old list. */
	for(size_t i=0; i < test_size; i++) push(list, i);

	/* have 16 threads suck it dry. */
	pthread_barrier_init(&launch_barrier, NULL, n_threads);
	pthread_t oth[n_threads];
	for(size_t i=0; i < n_threads; i++) {
		int n = pthread_create(&oth[i], NULL, &grab_items_fn, list);
		if(n != 0) {
			diag("pthread_create: n=%d", n);
			abort();
		}
	}

	/* get results. */
	darray(struct item *) *result[n_threads];
	for(size_t i=0; i < n_threads; i++) {
		void *ret = NULL;
		int n = pthread_join(oth[i], &ret);
		if(n != 0) {
			diag("pthread_join: i=%zu, n=%d", i, n);
			abort();
		}
		result[i] = ret;
		if(test_size / n_threads > 200 && result[i]->size < 100) {
			diag("result[%zu]->size=%zu", i, result[i]->size);
		}
	}

	/* analyze 'em. */
	ok(top(list) == NULL, "list was emptied");

	size_t total = 0;
	for(size_t i=0; i < n_threads; i++) total += result[i]->size;
	ok1(total == test_size);

	/* each thread should receive items in order of appearance, i.e. largest
	 * first.
	 */
	bool order = true;
	for(size_t i=0; i < n_threads; i++) {
		if(result[i]->size < 2) continue;
		for(size_t j=1, pval = result[i]->item[0]->value;
			j < result[i]->size;
			j++)
		{
			size_t cval = result[i]->item[j]->value;
			if(pval < cval) {
				diag("i=%zu, j=%zu, pval=%zu, cval=%zu", i, j, pval, cval);
				order = false;
				break;
			}
		}
	}
	ok(order, "each thread popped items in order");

	/* each item should appear exactly once. */
	darray(struct item *) all = darray_new();
	for(size_t i=0; i < n_threads; i++) {
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
	ok(uniq && all.size == test_size,
		"each item was popped exactly once");

	return exit_status();
}
