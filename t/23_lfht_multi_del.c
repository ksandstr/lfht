
/* test to tickle out deletions that succeed when they shouldn't, or fail when
 * they should succeed.
 *
 * inserts sequences of non-overlapping ranges of keys multiple times, then
 * deletes them. #dels = #adds, regardless of how migration runs in other
 * threads. deletions happen 1/3rd of the way through and again at the end.
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


#define NUM_STRINGS 1000
#define NUM_DUPLICATES 10


static _Atomic int next_thread_id = 0;
static pthread_barrier_t start_bar;


static size_t str_hash_fn(const void *key, void *priv) {
	return hashl(key, strlen(key), (uintptr_t)priv);
}


static bool cmp_str_ptr(const void *cand, void *ref) {
	return strcmp(cand, ref) == 0;
}


static bool str_in(struct lfht *ht, size_t hash, const char *str) {
	const char *s = lfht_get(ht,
		hash != 0 ? hash : str_hash_fn(str, NULL),
		&cmp_str_ptr, str);
	assert(s == NULL || strcmp(s, str) == 0);
	return s != NULL;
}


static char *gen_string(int seed)
{
	const size_t chunk_size = 64 * 1024;
	static __thread char *buffer = NULL;
	static __thread size_t bufsize, bufpos;

	if(buffer == NULL) {
		buffer = malloc(chunk_size);
		if(buffer == NULL) {
			diag("%s: malloc failed", __func__);
			abort();
		}
		bufsize = chunk_size;
		bufpos = 0;
	}
	bufpos = (bufpos + 7) & ~7ul;
	size_t n = snprintf(&buffer[bufpos], bufsize - bufpos,
		"test-%06x", seed);
	if(bufpos + n + 1 >= bufsize) {
		buffer = NULL;
		return gen_string(seed);
	} else {
		size_t p = bufpos;
		bufpos += n + 1;
		return &buffer[p];
	}
}


struct result {
	bool found, not_found, del_pos_ok, del_neg_ok;
};


static void *participant_fn(void *param_ptr)
{
	struct lfht *ht = param_ptr;
	struct result *res = malloc(sizeof(*res));
	res->found = true; res->not_found = true;
	res->del_pos_ok = true; res->del_neg_ok = true;

	const int thread_id = atomic_fetch_add(&next_thread_id, 1);
	char **strs = malloc(sizeof(char *) * NUM_STRINGS);
	for(size_t i=0; i < NUM_STRINGS; i++) {
		strs[i] = gen_string(thread_id * NUM_STRINGS + i);
	}
	int *n_present = calloc(NUM_STRINGS, sizeof(int));
	size_t *hashes = calloc(NUM_STRINGS, sizeof(size_t));

	int n = pthread_barrier_wait(&start_bar);
	if(n != 0 && n != PTHREAD_BARRIER_SERIAL_THREAD) {
		diag("%d: barrier wait failed, n=%d", thread_id, n);
		return NULL;
	}

	int eck = e_begin();
	size_t total_count = 0;
	bool del_neg_once = false;
	for(size_t i=0; i < NUM_DUPLICATES; i++) {
		for(size_t j=0; j < NUM_STRINGS; j++) {
			char *s = strs[j];
			if(i == 0) hashes[j] = str_hash_fn(s, NULL);
			size_t hash = hashes[j];
			if(res->not_found && n_present[j] == 0 && str_in(ht, hash, s)) {
				res->not_found = false;
				diag("%d: found `%s' when shouldn't (i=%zu, j=%zu)",
					thread_id, s, i, j);
			}
			bool ok = lfht_add(ht, hash, s);
			assert(ok);
			n_present[j]++;
			total_count++;
			if(res->found && !str_in(ht, hash, s)) {
				res->found = false;
				diag("%d: didn't find `%s' right after add (i=%zu, j=%zu)",
					thread_id, s, i, j);
			}

			if((total_count % 239) == 0) {
				e_end(eck);
				eck = e_begin();
			}
		}

		if(i == (NUM_DUPLICATES / 3) - 1 || i == NUM_DUPLICATES - 1) {
			/* delete all of 'em as many times as it takes to get to zero. */
			for(size_t j=0; j < NUM_STRINGS; j++) {
				char *s = strs[j];
				size_t hash = hashes[j];
				while(n_present[j] > 0) {
					bool ok = lfht_del(ht, hash, s);
					if(ok) n_present[j]--;
					else if(res->del_pos_ok && !ok) {
						diag("%d: didn't delete `%s' when should've (i=%zu, n_present[%zu]=%d)",
							thread_id, s, i, j, n_present[j]);
						res->del_pos_ok = false;
						break;
					}
				}

				/* and once more for the negative case, where possible. */
				if(n_present[j] == 0) {
					bool ok = lfht_del(ht, hash, s);
					if(res->del_neg_ok && ok) {
						diag("%d: deleted `%s' when shouldn't've (i=%zu, j=%zu)",
							thread_id, s, i, j);
						res->del_neg_ok = false;
					}
					del_neg_once = true;
				}
			}
		}
	}

	e_end(eck);

	free(n_present);
	free(strs);

	res->del_neg_ok &= del_neg_once;
	return res;
}


int main(void)
{
	const int num_threads = 32;

	diag("num_threads=%d, NUM_STRINGS=%d, NUM_DUPLICATES=%d",
		num_threads, NUM_STRINGS, NUM_DUPLICATES);
	plan_tests(4);

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
	struct result acc;
	for(int i=0; i < num_threads; i++) {
		void *resultptr = NULL;
		int n = pthread_join(ts[i], &resultptr);
		if(n != 0) {
			diag("pthread_join failed, n=%d", n);
			abort();
		}
		struct result *r = resultptr;
		if(i == 0) acc = *r;
		else {
			acc.found &= r->found; acc.not_found &= r->not_found;
			acc.del_pos_ok &= r->del_pos_ok; acc.del_neg_ok &= r->del_neg_ok;
		}
		free(r);
	}
	/* TODO: the not_found test requires some way to ensure that all
	 * concurrent migrations have completed, so an e_step() or something.
	 * that's currently absent, so the test should be regarded "not the end of
	 * the world, it'll sometimes do that" for now.
	 */
	todo_start("unstable (see comment)");
	ok(acc.not_found, "negative presence before add");
	todo_end();
	ok(acc.found, "positive presence after add");
	ok(acc.del_pos_ok, "positive deletion");
	ok(acc.del_neg_ok, "negative deletion");

	return exit_status();
}
