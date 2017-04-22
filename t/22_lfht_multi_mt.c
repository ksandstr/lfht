
/* test on using a large table concurrently as a multiset.
 *
 * generates a bunch of string keys and concurrently inserts overlapping
 * slices of the key array to a single lfht, then deletes them. the lfht is
 * left empty afterward. inserts happen multiple times per key per thread, and
 * deletion happens once during the insert loop and the other times at the
 * end.
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


#define NUM_STRINGS 34000
#define NUM_DUPLICATES 3


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


struct p_param {
	struct lfht *ht;
	char **strs;
	size_t first, num;
};


struct result {
	bool immed, del_ok, rest_del_ok;
};


static void *participant_fn(void *param_ptr)
{
	struct p_param *p = param_ptr;
	struct lfht *ht = p->ht;

	const int thread_id = atomic_fetch_add(&next_thread_id, 1);
	int n = pthread_barrier_wait(&start_bar);
	if(n != 0 && n != PTHREAD_BARRIER_SERIAL_THREAD) {
		diag("%d: barrier wait failed, n=%d", thread_id, n);
		return NULL;
	}

	int eck = e_begin();
	bool found_immed = true, del_ok = true;
	size_t n_dels = 0;
	for(size_t i=0, del_pos=0; i < p->num; i++) {
		char *s = p->strs[(p->first + i) % NUM_STRINGS];
		size_t hash = str_hash_fn(s, NULL);
		for(int j=0; j < NUM_DUPLICATES; j++) {
			bool ok = lfht_add(ht, hash, s);
			assert(ok);
			if(found_immed && !str_in(ht, hash, s)) {
				diag("%d: didn't find `%s' right after add j=%d",
					thread_id, s, j);
				found_immed = false;
			}
		}

		if(i - del_pos > 129 + thread_id || i == p->num - 1) {
			while(del_pos <= i) {
				s = p->strs[(p->first + del_pos) % NUM_STRINGS];
				bool ok = lfht_del(ht, str_hash_fn(s, NULL), s);
				if(!ok) {
					diag("%d: failed to delete `%s'", thread_id, s);
					del_ok = false;
				}
				del_pos++;
				n_dels++;
			}
		}

		if((i % 239) == 0) {
			e_end(eck);
			eck = e_begin();
		}
	}
	assert(n_dels == p->num);

	/* remove the other duplicates. */
	bool rest_del_ok = true;
	for(size_t i = 0; i < p->num; i++) {
		char *s = p->strs[(p->first + i) % NUM_STRINGS];
		size_t hash = str_hash_fn(s, NULL);
		for(int j=0; j < NUM_DUPLICATES - 1; j++) {
			bool ok = lfht_del(ht, hash, s);
			if(rest_del_ok && !ok) {
				diag("%d: failed to delete `%s' when j=%d",
					thread_id, s, j);
				rest_del_ok = false;
			}
		}
	}

	e_end(eck);

	struct result *r = malloc(sizeof(*r));
	*r = (struct result){
		.immed = found_immed,
		.del_ok = del_ok,
		.rest_del_ok = rest_del_ok,
	};
	free(p);
	return r;
}


int main(void)
{
	const int num_threads = 8, num_strings = NUM_STRINGS;

	diag("num_threads=%d, num_strings=%d, NUM_DUPLICATES=%d",
		num_threads, num_strings, NUM_DUPLICATES);
	plan_tests(4);

	struct lfht *ht = aligned_alloc(64, sizeof(*ht));
	lfht_init(ht, &str_hash_fn, NULL);
	char **strs = malloc(sizeof(char *) * num_strings);
	if(strs == NULL) abort();
	for(int i=0; i < num_strings; i++) strs[i] = gen_string(i);

	pthread_barrier_init(&start_bar, NULL, num_threads);
	pthread_t ts[num_threads];
	for(int i=0; i < num_threads; i++) {
		struct p_param *p = malloc(sizeof(*p));
		if(p == NULL) abort();
		*p = (struct p_param){
			.strs = strs, .ht = ht,
			.first = num_strings / (num_threads + 5) * i,
			.num = num_strings - (num_strings / num_threads) * 3,
		};
		diag("%d: first=%zu, num=%zu", i, p->first, p->num);
		int n = pthread_create(&ts[i], NULL, &participant_fn, p);
		if(n != 0) {
			diag("pthread_create failed, n=%d", n);
			abort();
		}
	}

	/* no thread should observe failure. */
	bool found_immed = true, del_ok = true, rest_del_ok = true;
	for(int i=0; i < num_threads; i++) {
		void *resultptr = NULL;
		int n = pthread_join(ts[i], &resultptr);
		if(n != 0) {
			diag("pthread_join failed, n=%d", n);
			abort();
		}
		struct result *r = resultptr;
		found_immed = found_immed && r->immed;
		del_ok = del_ok && r->del_ok;
		rest_del_ok = rest_del_ok && r->rest_del_ok;
		free(r);
	}
	ok(found_immed, "test strings were found immediately");
	ok1(del_ok);	/* delete succeeded on each key while adding */
	ok1(rest_del_ok);	/* ... and on each duplicate key afterward */

	int eck = e_begin();
	/* ht should be left empty. since lfht_first and lfht_next are
	 * unimplemented in this branch, we'll query each key separately. that's
	 * good enough.
	 */
	bool none_found = true;
	for(size_t i=0; i < num_strings; i++) {
		if(str_in(ht, 0, strs[i])) {
			none_found = false;
			diag("found i=%zu, s=`%s' after joins", i, strs[i]);
			break;
		}
	}
	ok(none_found, "hash table was empty after joins");

	lfht_clear(ht);
	e_end(eck);

	return exit_status();
}
