
/* non-blocking singly-linked lists. insert supported at the top ("push"),
 * deletion at any point. the intrusive link structure ("nbsl_node") must be
 * aligned to 8; due to pointer fuckery, structures pointed to solely with
 * this mechanism might not show up as reachable in valgrind etc.
 *
 * pointer lifetime safety left up to caller. code should expect to see
 * not-quite-deleted nodes during iteration in use cases that run concurrently
 * with nbsl_pop() or nbsl_del().
 */

#ifndef NBSL_H
#define NBSL_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>


struct nbsl_node {
	_Atomic uintptr_t next;
} __attribute__((aligned(8)));


struct nbsl {
	struct nbsl_node n;
};


#define NBSL_LIST_INIT(name) { { .next = 0 } }


static inline void nbsl_init(struct nbsl *list)
{
	atomic_store_explicit(&list->n.next, 0, memory_order_release);
}


/* push @n at the head of @list, if the previous head == @top. returns true if
 * successful, false if the caller should refetch @top and try again.
 */
extern bool nbsl_push(
	struct nbsl *list, struct nbsl_node *top, struct nbsl_node *n);

/* pop first node from @list, returning it or NULL. */
extern struct nbsl_node *nbsl_pop(struct nbsl *list);

/* peek first node in @list, returning it or NULL. */
extern struct nbsl_node *nbsl_top(struct nbsl *list);

/* remove @n from @list. O(n).
 *
 * returns true if the current thread removed @n from @list; false if some
 * other thread did it, or if removal was deferred due to concurrent access of
 * the previous node. in the first case @n will have gone away once nbsl_del()
 * returns; in the second, @n will have been removed from @list once every
 * concurrent call to nbsl_del() and nbsl_push() have returned.
 */
extern bool nbsl_del(struct nbsl *list, struct nbsl_node *n);

extern struct nbsl_node *nbsl_first(const struct nbsl *list);
extern struct nbsl_node *nbsl_next(const struct nbsl_node *node);


#endif