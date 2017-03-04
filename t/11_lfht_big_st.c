
/* test on creating a large table and periodically querying to see if all
 * items are present.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <ccan/tap/tap.h>
#include <ccan/hash/hash.h>

#include "epoch.h"
#include "lfht.h"


static size_t str_hash_fn(const void *key, void *priv) {
	return hash_string(key);
}


static bool cmp_str_ptr(const void *cand, void *ref) {
	return strcmp(cand, ref) == 0;
}


static bool str_in(struct lfht *ht, const char *str) {
	const char *s = lfht_get(ht, hash_string(str), &cmp_str_ptr, str);
	assert(s == NULL || strcmp(s, str) == 0);
	return s != NULL;
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
	struct lfht ht = LFHT_INITIALIZER(ht, &str_hash_fn, NULL);
	bool found_immed = true, found_delay = true;
	for(int i=0; i < 10000; i++) {
		char *s = gen_string(i);
		bool ok = lfht_add(&ht, hash_string(s), s);
		assert(ok);
		if(found_immed && !str_in(&ht, s)) {
			diag("didn't find `%s' right after add", s);
			found_immed = false;
		}
		if(found_delay && (i % 37) == 0) {
			for(int j=0; j <= i; j += 1 + i / 49) {
				s = gen_string(j);
				if(!str_in(&ht, s)) {
					diag("didn't find `%s' at i=%d", s, i);
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
	pass("add loop didn't crash");
	ok(found_immed, "test strings were found immediately");
	ok(found_delay, "test strings were found with delay");

	lfht_clear(&ht);
	e_end(eck);

	return exit_status();
}

