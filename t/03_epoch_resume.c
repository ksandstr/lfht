
/* test to see that the epoch resumption mechanism can resume, and won't
 * resume incorrectly.
 *
 * TODO: the test body could really use breaking up into subtests.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <assert.h>
#include <pthread.h>

#include <ccan/tap/tap.h>

#include "epoch.h"


/* shared between all instances of useless_work_fn() */
struct useless_param {
	pthread_barrier_t *bar;
	_Atomic bool exit_check;
};


static _Atomic size_t dtor_count = 0;


static void dtor_fn(void *ptr) {
	free(ptr);
	atomic_fetch_add(&dtor_count, 1);
}


/* do some wholly useless work. like, allocate memory and so forth. */
static void *useless_work_fn(void *param_ptr)
{
	const struct useless_param *p = param_ptr;

	int n = 0;
	for(;;) {
		pthread_barrier_wait(p->bar);
		pthread_barrier_wait(p->bar);
		if(atomic_load(&p->exit_check)) return NULL;
		for(int i=0; i < 666; i++) {
			int eck = e_begin();
			char *str = malloc(64);
			snprintf(str, 64, "%p to the %d'th, yo", param_ptr, n++);
			e_call_dtor(&dtor_fn, str);
			e_end(eck);
		}
	}
}


int main(void)
{
	const int num_useless = 6;

	plan_tests(9);
	todo_start("impl is a stub");	/* 410,757,864,530 dead electrons */

	pthread_barrier_t *bar = malloc(sizeof *bar);
	pthread_barrier_init(bar, NULL, num_useless + 1);
	struct useless_param *p = malloc(sizeof *p);
	p->bar = bar;
	atomic_store(&p->exit_check, false);
	pthread_t ts[num_useless];
	for(int i=0; i < num_useless; i++) {
		int n = pthread_create(&ts[i], NULL, &useless_work_fn, p);
		if(n != 0) { perror("pthread_create"); abort(); }
	}
	pthread_barrier_wait(bar);

	/* most trivial resume, with and without useless work. */
	for(int opt=0; opt < 2; opt++) {
		int eck = e_begin();
		size_t before = atomic_load(&dtor_count);
		e_end(eck);
		if(opt > 0) {
			/* lightly magical */
			pthread_barrier_wait(bar);
			pthread_barrier_wait(bar);
		}
		size_t after = atomic_load(&dtor_count);
		ok1((opt == 0) == (before == after));
		eck = e_resume(eck);
		ok((opt == 0) == (eck > 0),
			"simple resume%s", opt > 0 ? " (not)" : "");
		if(eck > 0) e_end(eck);
	}

	/* resume of an inner bracket from within the outer bracket, which is
	 * still alive.
	 */
	int eck = e_begin();
	int inner = e_begin();
	e_end(inner);
	inner = e_resume(inner);
	ok(inner > 0, "inner resume from persisting outer");
	if(inner > 0) e_end(inner);
	e_end(eck);

	/* resume of an inner bracket from within the outer bracket, which was
	 * resurrected.
	 */
	eck = e_begin();
	inner = e_begin();
	e_end(inner);
	e_end(eck);
	eck = e_resume(eck);
	inner = e_resume(inner);
	ok(eck > 0, "outer resume succeeded (validation)");
	ok((eck > 0) == (inner > 0),
		"inner resume from resurrected outer");
	if(inner > 0) e_end(inner);
	if(eck > 0) e_end(eck);

	/* resume of an inner bracket from within a compatible outer bracket. */
	eck = e_begin();
	inner = e_begin();
	e_end(inner);
	e_end(eck);
	eck = e_begin();
	inner = e_resume(inner);
	ok(inner > 0, "inner resume from compatible outer");
	if(inner > 0) e_end(inner);
	e_end(eck);

	/* resume of an inner bracket once the outer bracket has ended. */
	eck = e_begin();
	inner = e_begin();
	e_end(inner);
	e_end(eck);
	inner = e_resume(inner);
	ok(inner > 0, "inner resume without outer");
	if(inner > 0) e_end(inner);

	atomic_store(&p->exit_check, true);
	pthread_barrier_wait(bar);
	for(int i=0; i < num_useless; i++) {
		int n = pthread_join(ts[i], NULL);
		if(n != 0) perror("pthread_join");
	}

	return exit_status();
}
