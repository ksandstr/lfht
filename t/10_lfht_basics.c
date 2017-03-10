
/* basic interface tests on lfht.h: init, sized init, add, get, del. */

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


int main(void)
{
	plan_tests(5 + 6);

	/* you, you double initializer. */
	struct lfht ht = LFHT_INITIALIZER(ht, &str_hash_fn, NULL);
	lfht_clear(&ht);
	pass("survived clear after LFHT_INITIALIZER()");

	memset(&ht, 0xfe, sizeof(ht));
	lfht_init(&ht, &str_hash_fn, NULL);
	lfht_clear(&ht);
	pass("survived clear after lfht_init()");

	static const int sizes[3] = { 5, 123, 12345 };
	for(int i=0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
		memset(&ht, 0xfd - i, sizeof(ht));
		lfht_init_sized(&ht, &str_hash_fn, NULL, sizes[i]);
		bool ok = lfht_add(&ht, hash_string("foo"), "foo");
		assert(ok);
		ok = lfht_add(&ht, hash_string("bar"), "bar");
		assert(ok);
		lfht_clear(&ht);
		pass("survived simple adds after lfht_init_sized(..., %d)", sizes[i]);
	}

	/* get, add, get, del, and get again. */
	int eck = e_begin();
	lfht_init(&ht, &str_hash_fn, NULL);
	ok1(!str_in(&ht, "foo"));
	bool ok = lfht_add(&ht, hash_string("foo"), "foo");
	ok(ok, "add `foo'");
	ok1(str_in(&ht, "foo"));
	ok = lfht_del(&ht, hash_string("foo"), "foo");
	ok(ok, "del `foo'");
	ok = lfht_del(&ht, hash_string("foo"), "foo");
	ok(!ok, "!del `foo'");
	ok1(!str_in(&ht, "foo"));
	lfht_clear(&ht);
	e_end(eck);

	return exit_status();
}
