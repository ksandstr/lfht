
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <assert.h>

#include "nbsl.h"


#define F_RESERVED 1
#define F_DEAD 2
#define F_MASK ((uintptr_t)3)


static inline struct nbsl_node *n_ptr(uintptr_t x) {
	return (struct nbsl_node *)(x & ~F_MASK);
}


bool nbsl_push(struct nbsl *list, struct nbsl_node *top, struct nbsl_node *n)
{
	assert(((uintptr_t)n & F_MASK) == 0);
	uintptr_t old = atomic_load_explicit(&list->n.next, memory_order_relaxed);
	assert((old & F_MASK) == 0);
	n->next = old;
	return n_ptr(old) == top
		&& atomic_compare_exchange_strong_explicit(&list->n.next,
			&old, (uintptr_t)n, memory_order_release, memory_order_relaxed);
}


/* mark @n per nbsl_del() protocol. *@nextptr is a value of @n->next, reloaded
 * if the cmpxchg fails. returns true when we set F_RESERVED on @n, and false
 * when @n->next == 0 or another thread already set F_RESERVED or F_DEAD.
 */
static inline bool del_mark(struct nbsl_node *n, uintptr_t *nextptr)
{
	assert((*nextptr & F_DEAD) == 0);
	while((*nextptr & F_MASK) == 0) {
		if(atomic_compare_exchange_strong(&n->next,
			nextptr, *nextptr | F_RESERVED))
		{
			*nextptr |= F_RESERVED;
			return true;
		}
	}
	return false;
}


static inline void del_complete(
	_Atomic uintptr_t *pp, uintptr_t headptr,
	struct nbsl_node *n, uintptr_t nextptr)
{
	assert(n_ptr(headptr) == n);
	assert((headptr & F_RESERVED) == 0);
	assert((headptr & F_DEAD) == 0);
	/* @nextptr must be value of earlier atomic_load(&n->next). */
	assert((nextptr & F_MASK) == F_RESERVED);

	/* remove @n from @pp. clean bits if removal was successful, i.e. not
	 * broken by a concurrent insert or completed by another thread.
	 */
	if(atomic_compare_exchange_strong(pp, &headptr, nextptr & ~F_MASK)
		|| (headptr & ~F_MASK) == (nextptr & ~F_MASK))
	{
		/* change R -> D and ignore the result, per nbsl_del() protocol. */
		atomic_compare_exchange_strong(&n->next,
			&nextptr, (nextptr & ~F_RESERVED) | F_DEAD);
		assert((atomic_load(&n->next) & F_MASK) == F_DEAD);
	}
}


/* remove @n, which was at @list head, where @list->n.next was @headptr.
 * return true iff @n wasn't removed already, and this thread was the one to
 * mark @n for removal.
 */
static bool take_head(
	struct nbsl *list, uintptr_t headptr, struct nbsl_node *n)
{
	uintptr_t nextptr = atomic_load_explicit(&n->next, memory_order_consume);
	if((nextptr & F_DEAD) != 0) {
		/* move the next item over where possible & signal failure to
		 * grab @n.
		 */
		atomic_compare_exchange_strong(&list->n.next, &headptr,
			nextptr & ~F_MASK);
		return false;
	}

	/* try to mark it per nbsl_del() protocol. */
	bool got = del_mark(n, &nextptr);
	if(!got && (nextptr & F_DEAD) != 0) return false;

	/* ensure that removal completed. */
	assert((nextptr & F_RESERVED) != 0);
	del_complete(&list->n.next, headptr, n, nextptr);

	return got;
}


struct nbsl_node *nbsl_pop(struct nbsl *list)
{
	uintptr_t headptr;
retry:
	headptr = atomic_load_explicit(&list->n.next, memory_order_consume);
	if(headptr == 0) return NULL;
	assert((headptr & F_MASK) == 0);

	struct nbsl_node *n = n_ptr(headptr);
	if(!take_head(list, headptr, n)) goto retry;
	return n;
}


struct nbsl_node *nbsl_top(struct nbsl *list) {
	return n_ptr(atomic_load_explicit(&list->n.next, memory_order_consume));
}


bool nbsl_del(struct nbsl *list, struct nbsl_node *target)
{
	uintptr_t headptr;
retry:
	headptr = atomic_load_explicit(&list->n.next, memory_order_consume);
	if(headptr == 0) return false;
	struct nbsl_node *p = n_ptr(headptr);
	if(p == target) return take_head(list, headptr, p);

	/* find P -> X. */
	uintptr_t p_val = atomic_load(&p->next), n_val;
	struct nbsl_node *n;
	while(p_val != 0) {
		n = n_ptr(p_val);
		n_val = atomic_load(&n->next);
		if(n == target) break;
		if((p_val & (F_RESERVED | F_DEAD)) == 0
			&& (n_val & F_RESERVED) != 0)
		{
			/* complete an in-progress deletion and repeat. */
			del_complete(&p->next, p_val, n, n_val);
			p_val = atomic_load(&p->next);
		} else {
			p = n;
			p_val = n_val;
		}
	}
	if(p_val == 0) return false;
	if((p_val & F_DEAD) != 0) goto retry;

	/* try to mark, try to delete if permitted. */
	bool got = del_mark(n, &n_val);
	if(!got && (n_val & F_DEAD) != 0) return false;
	if((p_val & F_RESERVED) == 0) {
		assert((n_val & F_RESERVED) != 0);
		del_complete(&p->next, p_val, n, n_val);
	}

	return got;
}


struct nbsl_node *nbsl_first(const struct nbsl *list)
{
	return NULL;
}


struct nbsl_node *nbsl_next(const struct nbsl_node *node)
{
	return NULL;
}
