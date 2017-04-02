
/* test on creating a large table and periodically querying to see if all
 * items are present; multithreaded version where each thread runs on a
 * distinct keyset using the same hash table.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/hash/hash.h>

#include "epoch.h"
#include "lfht.h"


static _Atomic int next_thread_id = 0;
static pthread_barrier_t start_bar;


static size_t str_hash_fn(const void *key, void *priv) {
	return hashl(key, strlen(key), (uintptr_t)priv);
}


static bool cmp_str_ptr(const void *cand, void *ref) {
	return strcmp(cand, ref) == 0;
}


static bool str_in(struct lfht *ht, const char *str) {
	const char *s = lfht_get(ht, str_hash_fn(str, NULL),
		&cmp_str_ptr, str);
	assert(s == NULL || strcmp(s, str) == 0);
	return s != NULL;
}


static char *gen_string(int thread_id, int seed)
{
	char buf[100];
	snprintf(buf, sizeof(buf), "test-tid%02d-%04x", thread_id, seed);
	return strdup(buf);
}


struct result {
	bool before, immed, delay;
};


static void *participant_fn(void *param_ptr)
{
	const int thread_id = atomic_fetch_add(&next_thread_id, 1);
	struct lfht *ht = param_ptr;
	int n = pthread_barrier_wait(&start_bar);
	if(n != 0 && n != PTHREAD_BARRIER_SERIAL_THREAD) {
		diag("%d: barrier wait failed, n=%d", thread_id, n);
		return NULL;
	}

	int eck = e_begin();
	bool found_before = false, found_immed = true, found_delay = true;
	for(int i=0; i < 10000; i++) {
		char *s = gen_string(thread_id, i);
		if(!found_before && str_in(ht, s)) {
			diag("%d: found `%s' before it was added", thread_id, s);
			found_before = true;
		}
		bool ok = lfht_add(ht, str_hash_fn(s, NULL), s);
		assert(ok);
		if(found_immed && !str_in(ht, s)) {
			diag("%d: didn't find `%s' right after add", thread_id, s);
			found_immed = false;
		}
		if(found_delay && (i % 37) == 0) {
			for(int j=0; j <= i; j += 1 + i / 49) {
				s = gen_string(thread_id, j);
				if(!str_in(ht, s)) {
					diag("%d: didn't find `%s' at i=%d", thread_id, s, i);
					found_delay = false;
				}
				free(s);
			}
		}
		if((i % 239) == 0) {
			e_end(eck);
			eck = e_begin();
		}
	}
	e_end(eck);

	struct result *r = malloc(sizeof(*r));
	*r = (struct result){
		.before = found_before,
		.immed = found_immed,
		.delay = found_delay,
	};
	return r;
}


int main(void)
{
	const int num_threads = 8;

	diag("num_threads=%d", num_threads);
	plan_tests(3);

	struct lfht *ht = aligned_alloc(64, sizeof(*ht));
	lfht_init(ht, &str_hash_fn, NULL);
	pthread_barrier_init(&start_bar, NULL, num_threads);
	pthread_t ts[num_threads];
	for(int i=0; i < num_threads; i++) {
		int n = pthread_create(&ts[i], NULL, &participant_fn, ht);
		if(n != 0) {
			diag("pthread_create failed, n=%d", n);
			abort();
		}
	}

	/* no thread should observe failure. */
	bool found_before = true, found_immed = true, found_delay = true;
	for(int i=0; i < num_threads; i++) {
		void *resultptr = NULL;
		int n = pthread_join(ts[i], &resultptr);
		if(n != 0) {
			diag("pthread_join failed, n=%d", n);
			abort();
		}
		struct result *r = resultptr;
		found_before = found_before && r->before;
		found_immed = found_immed && r->immed;
		found_delay = found_delay && r->delay;
		free(r);
	}
	ok(!found_before, "test strings weren't found before add");
	ok(found_immed, "test strings were found immediately");
	ok(found_delay, "test strings were found with delay");

	lfht_clear(ht);

	return exit_status();
}
