
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


int main(void)
{
	plan_tests(5);
	todo_start("no implementation!");

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
		lfht_clear(&ht);
		pass("survived clear after lfht_init_sized(..., %d)", sizes[i]);
	}

	/* TODO: tests on add, get, del. */

	return exit_status();
}
