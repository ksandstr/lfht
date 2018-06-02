
/* test on creating a large table and periodically iterating over it all to
 * confirm that complete iteration returns all items. multithreaded, each
 * thread runs for a distinct keyset in the same hash table.
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
#include <ccan/htable/htable.h>

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


static bool str_in(struct htable *ht, const char *str) {
	const char *s = htable_get(ht, str_hash_fn(str, NULL),
		&cmp_str_ptr, str);
	assert(s == NULL || strcmp(s, str) == 0);
	return s != NULL;
}


/* FIXME: copypasta'd from 21_lfht_big_mt! move it somewhere common. */
static char *gen_string(int thread_id, int seed)
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
		"test-tid%02d-%04x", thread_id, seed);
	if(bufpos + n + 1 >= bufsize) {
		buffer = NULL;
		return gen_string(thread_id, seed);
	} else {
		size_t p = bufpos;
		bufpos += n + 1;
		return &buffer[p];
	}
}


static void add_snapshot_items(struct htable *snapshot, struct lfht *ht)
{
	struct lfht_iter it;
	for(const char *cur = lfht_first(ht, &it);
		cur != NULL;
		cur = lfht_next(ht, &it))
	{
		bool ok = htable_add(snapshot, str_hash_fn(cur, NULL), cur);
		if(!ok) {
			printf("%s: can't add `%s' to snapshot (out of memory?)\n",
				__func__, cur);
			abort();
		}
	}
}


struct result {
	bool before, delay;
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

	struct htable snapshot;
	htable_init(&snapshot, &str_hash_fn, NULL);

	int eck = e_begin();
	bool found_before = false, found_delay = true;
	for(int i=0, limit = 10000; i < limit; i++) {
		char *s = gen_string(thread_id, i);
		if(!found_before && str_in(&snapshot, s)) {
			diag("%d: found `%s' before it was added", thread_id, s);
			found_before = true;
		}
		bool ok = lfht_add(ht, str_hash_fn(s, NULL), s);
		assert(ok);
		if(found_delay && ((i % 223) == 0 || i == limit - 1)) {
			htable_clear(&snapshot);
			add_snapshot_items(&snapshot, ht);
			for(int j=0; j <= i; j += 1 + i / 49) {
				s = gen_string(thread_id, j);
				if(!str_in(&snapshot, s)) {
					diag("%d: didn't find `%s' at i=%d", thread_id, s, i);
					found_delay = false;
				}
			}
		}
		if((i % 239) == 0) {
			e_end(eck);
			eck = e_begin();
		}
	}
	e_end(eck);
	htable_clear(&snapshot);

	struct result *r = malloc(sizeof(*r));
	*r = (struct result){
		.before = found_before,
		.delay = found_delay,
	};
	return r;
}


int main(void)
{
	const int num_threads = 8;

	diag("num_threads=%d", num_threads);
	plan_tests(2);

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
	bool found_before = false, found_delay = true;
	for(int i=0; i < num_threads; i++) {
		void *resultptr = NULL;
		int n = pthread_join(ts[i], &resultptr);
		if(n != 0) {
			diag("pthread_join failed, n=%d", n);
			abort();
		}
		struct result *r = resultptr;
		found_before = found_before || r->before;
		found_delay = found_delay && r->delay;
		free(r);
	}
	ok(!found_before, "snapshot didn't have strings before add");
	ok(found_delay, "snapshot did have strings after add");

	lfht_clear(ht);

	return exit_status();
}
