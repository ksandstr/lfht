
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
	plan_tests(10);
	todo_start("wheeee");

	ok1(!e_inside());
	int eck = e_begin();
	ok1(eck >= 0);
	ok1(e_inside());
	e_end(eck);
	pass("e_end() didn't panic");
	ok1(!e_inside());

	eck = e_begin();
	assert(eck >= 0);
	int tck = e_torpor();
	ok1(tck >= 0);
	ok1(e_inside());
	int n = e_rouse(tck);
	ok(n == 0 || n == -EAGAIN,
		"e_rouse() on valid cookie ok");
	e_end(eck);
	ok1(!e_inside());

	tck = e_torpor();
	ok(tck == -EINVAL, "e_torpor() invalid outside e_begin()");

	return exit_status();
}
