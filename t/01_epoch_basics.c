
/* basic interface tests on the epoch mechanism, i.e. that return values are
 * somewhat in the right ballpark.
 */

#include <errno.h>
#include <assert.h>

#include <ccan/tap/tap.h>

#include "epoch.h"


int main(void)
{
	plan_tests(5);

	ok1(!e_inside());
	int eck = e_begin();
	ok1(eck >= 0);
	ok1(e_inside());
	e_end(eck);
	pass("e_end() didn't panic");
	ok1(!e_inside());

	return exit_status();
}
