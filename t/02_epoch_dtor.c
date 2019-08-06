
/* tests on the epoch destructor functions: that e_call_dtor() calls the
 * destructor only after all epoch brackets have exited, and that the
 * destructor begins to get called as later epoch brackets are opened and
 * closed.
 */

#include <stdlib.h>
#include <stdatomic.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>

#include "epoch.h"


static bool in_danger = false;
static _Atomic int dtor_calls = 0, dtor_in_danger = 0;


static void dtor_check(char *stringu)
{
	atomic_fetch_add(&dtor_calls, 1);
	if(in_danger) {
		atomic_fetch_add(&dtor_in_danger, 1);
		diag("in_danger=true w/ stringu=`%s'", stringu);
	}
}


static void *other_thread_fn(void *barptr)
{
	pthread_barrier_t *bar = barptr;
	int eck = e_begin();
	pthread_barrier_wait(bar);
	e_call_dtor(dtor_check, "other");
	e_end(eck);

	return NULL;
}


static void *spam_thread_fn(void *unused)
{
	for(int i=0; i < 666; i++) {
		void *spurious = calloc(123 + i, 3);
		int eck = e_begin();
		e_free(spurious);
		e_end(eck);

		if(i % 50 == 0) usleep(500);
	}

	return NULL;
}


int main(void)
{
	plan_tests(3);

	pthread_barrier_t *bar = malloc(sizeof *bar);
	int n = pthread_barrier_init(bar, NULL, 2);
	assert(n == 0);
	pthread_t other;
	n = pthread_create(&other, NULL, &other_thread_fn, bar);
	if(n < 0) {
		diag("pthread_create() failed, n=%d, errno=%d", n, errno);
		return EXIT_FAILURE;
	}

	int eck = e_begin();
	assert(eck >= 0);
	in_danger = true;
	usleep(20);
	e_call_dtor(&dtor_check, "main");
	pthread_t spams[12];
	for(int i=0; i < 12; i++) {
		pthread_create(&spams[i], NULL, &spam_thread_fn, NULL);
	}
	ok1(dtor_calls == 0);
	in_danger = false;
	e_end(eck);
	pthread_barrier_wait(bar);

	for(int i=0; i < 4; i++) {
		eck = e_begin();
		assert(eck >= 0);
		/* look mom, I can spin */
		e_end(eck);
	}
	pthread_join(other, NULL);

	/* pick up the entrails */
	ok(dtor_calls > 0, "dtor should've been called");
	ok1(dtor_in_danger == 0);

	for(int i=0; i < 12; i++) pthread_join(spams[i], NULL);
	pthread_barrier_destroy(bar);
	free(bar);

	return exit_status();
}
