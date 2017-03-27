
/* test on creating a large table with many deletions, and querying it to
 * confirm contents. single-threaded version.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <ccan/tap/tap.h>
#include <ccan/hash/hash.h>
#include <ccan/htable/htable.h>
#include <ccan/container_of/container_of.h>

#include "epoch.h"
#include "lfht.h"


static size_t str_hash_fn(const void *key, void *priv) {
	return hash_string(key);
}


static char *gen_string(int seed)
{
	char buf[100];
	snprintf(buf, sizeof(buf), "test-%04x", seed);
	return strdup(buf);
}


int main(void)
{
	plan_tests(3);

	int eck = e_begin();
	struct htable st_contents = HTABLE_INITIALIZER(
		st_contents, &str_hash_fn, NULL);
	struct lfht ht = LFHT_INITIALIZER(ht, &str_hash_fn, NULL);
	bool del_ok = true, add_ok = true, saw_rehash = false;
	for(int i=0; i < 12000; i++) {
		char *s = gen_string(i);
		size_t hash = hash_string(s);
		bool ok = lfht_add(&ht, hash, s);
		if(!ok && add_ok) {
			diag("add of `%s' failed", s);
			add_ok = false;
		}
		ok = htable_add(&st_contents, hash, s);
		assert(ok);

		bool del = (i > 1700 && i < 3000 && (i & 3) == 0)
			|| (i >= 3000 && i < 8500 && (i & 1) == 0);
		int n_del = i >= 8500 && (i & 3) != 0 ? 3 : 1;
		if(del || n_del > 1) {
			for(int j=0; j < n_del; j++) {
				struct htable_iter pick;
				s = htable_first(&st_contents, &pick);
				assert(s != NULL);
				htable_delval(&st_contents, &pick);
				hash = hash_string(s);
				ok = lfht_del(&ht, hash, s);
				if(!ok && del_ok) {
					diag("del of `%s' failed", s);
					del_ok = false;
				}
				e_free(s);
			}
		}

		if((i % 239) == 0) {
			e_end(eck);
			eck = e_begin();
		}

		if(!saw_rehash) {
			struct nbsl_iter it;
			struct lfht_table *t = container_of(nbsl_first(&ht.tables, &it),
					struct lfht_table, link),
				*next = container_of(nbsl_next(&ht.tables, &it),
					struct lfht_table, link);
			if(next != NULL && next->size_log2 == t->size_log2) {
				saw_rehash = true;
			}
		}
	}

	ok1(saw_rehash);
	ok1(add_ok);
	ok1(del_ok);

	lfht_clear(&ht);
	e_end(eck);
	htable_clear(&st_contents);

	return exit_status();
}
