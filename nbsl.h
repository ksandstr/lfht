
/* non-blocking singly-linked lists.
 *
 * the algorithm used was presented by Fomitchev and Ruppert in ``Lock-Free
 * Linked Lists and Skip Lists'' [York University, 2003].
 *
 * this implementation supports insert at the list head ("push"), deletion at
 * any point, and iteration. the intrusive link structure ("nbsl_node") must
 * be aligned to 4; due to storage of metadata in the low bits, structures
 * pointed to solely with this mechanism might not show up as reachable in
 * valgrind etc.
 */

#ifndef NBSL_H
#define NBSL_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>


struct nbsl_node {
	_Atomic uintptr_t next;
	struct nbsl_node *_Atomic backlink;
} __attribute__((aligned(4)));


struct nbsl {
	struct nbsl_node n;
};


#define NBSL_LIST_INIT(name) { { .next = 0, .backlink = NULL } }


static inline void nbsl_init(struct nbsl *list) {
	struct nbsl proto = NBSL_LIST_INIT(proto);
	*list = proto;
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


struct nbsl_iter {
	struct nbsl_node *prev, *cur;
};

/* iteration. this is always read-only, i.e. never causes writes to any node
 * along the chain. it skips over dead nodes, but the ones it returns may
 * appear dead nonetheless due to concurrent delete.
 */
extern struct nbsl_node *nbsl_first(struct nbsl *list, struct nbsl_iter *it);
extern struct nbsl_node *nbsl_next(struct nbsl *list, struct nbsl_iter *it);

/* attempt to remove value returned from previous call to nbsl_{first,next}(),
 * returning true on success and false on failure. @it remains robust against
 * concurrent mutation; subsequent calls to nbsl_del_at() before nbsl_next()
 * always return false.
 *
 * a sequence of nbsl_del_at() and nbsl_next() can be used to pop all nodes
 * from @list from a certain point onward.
 */
extern bool nbsl_del_at(struct nbsl *list, struct nbsl_iter *it);

#endif
