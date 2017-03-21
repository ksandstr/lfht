
/* basic interface tests on the non-blocking singly-linked lists. */

#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>
#include <ccan/container_of/container_of.h>

#include "nbsl.h"


struct item {
	struct nbsl_node link;
	int value;
};


static struct item *push(struct nbsl *list, int value)
{
	struct item *it = malloc(sizeof(*it));
	it->value = value;
	int spins = 0;
	while(!nbsl_push(list, nbsl_top(list), &it->link)) {
		/* spin */
		diag("%s: repeating", __func__);
		if(++spins == 10) abort();
	}
	return it;
}


static struct item *pop(struct nbsl *list)
{
	struct nbsl_node *link = nbsl_pop(list);
	return container_of_or_null(link, struct item, link);
}


static struct item *top(struct nbsl *list)
{
	struct nbsl_node *link = nbsl_top(list);
	return container_of_or_null(link, struct item, link);
}


int main(void)
{
	plan_tests(13);

	struct nbsl *list = malloc(sizeof(*list));
	nbsl_init(list);
	struct item *n1 = push(list, 1);
	push(list, 2);
	push(list, 3);
	push(list, 4);

	/* test "pop" while items exist. */
	ok1(top(list) != NULL);
	ok1(pop(list)->value == 4);
	ok1(top(list) != NULL);
	ok1(top(list)->value == 3);

	/* test nbsl_del() from top. */
	ok1(nbsl_del(list, nbsl_top(list)));
	ok1(top(list) != NULL);
	ok1(top(list)->value == 2);

	/* test nbsl_del() from bottom. */
	ok1(nbsl_del(list, &n1->link));
	ok1(top(list) != NULL);
	ok1(top(list)->value == 2);

	/* popping the last element */
	ok1(pop(list)->value == 2);
	ok1(top(list) == NULL);
	ok1(pop(list) == NULL);

	return exit_status();
}
