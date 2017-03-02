
/* basic interface tests on the epoch mechanism, i.e. that return values are
 * somewhat in the right ballpark.
 */

#include <errno.h>
#include <assert.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>

#include "epoch.h"


int main(void)
{
	plan_tests(5);
	todo_start("wheeee");

	int eck = e_begin();
	ok1(eck >= 0);
	e_end(eck);
	pass("e_end() didn't panic");

	eck = e_begin();
	assert(eck >= 0);
	int tck = e_torpor();
	ok1(tck >= 0);
	int n = e_rouse(tck);
	ok(n == 0 || n == -EAGAIN,
		"e_rouse() on valid cookie ok");
	e_end(eck);

	tck = e_torpor();
	ok(tck == -EINVAL, "e_torpor() invalid outside e_begin()");

	return exit_status();
}
