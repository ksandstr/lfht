
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <limits.h>
#include <assert.h>
#include <errno.h>

#include "epoch.h"


struct e_client
{
	/* concurrent access per item w/ stdatomic. */
	struct e_client *_Atomic next;
	_Atomic unsigned long epoch;	/* 0 for idle. */

	bool initialized;
};


struct e_dtor_call
{
	struct e_dtor_call *_Atomic next;
	void (*dtor_fn)(void *ptr);
	void *ptr;
};


static struct e_client *_Atomic client_list = NULL;

static _Atomic unsigned long global_epoch = 2;

/* [epoch + 1 mod 4] = NULL
 * [epoch     mod 4] = fresh dtors, current insert position
 * [epoch - 1 mod 4] = quiet dtors, possibly still under access
 * [epoch - 2 mod 4] = in-progress dtors (then NULL)
 *
 * TODO: split each of these up 4/16/64 ways & index with shifted cpu# to put
 * sibling threads on the same slot. the current setup guarantees maximum
 * pingpong.
 */
static struct e_dtor_call *_Atomic epoch_dtors[4];
static _Atomic unsigned long epoch_counts[4];


static void client_dtor(void *clientptr)
{
	struct e_client *c = clientptr;

	/* find parent pointer. */
	struct e_client *_Atomic *pp = &client_list;
	for(;;) {
		struct e_client *prev = atomic_load_explicit(pp, memory_order_relaxed);
		if(prev == c) break;
		if(prev == NULL) {
			/* the list will never not have items that should be there. */
			fprintf(stderr, "epoch: %s: shouldn't happen!\n", __func__);
			abort();
		}
		pp = &prev->next;
	}

	/* basic list removal. we can exchange instead of cmpxchg because it's
	 * known that only the thread in client_dtor(@clientptr) will write the
	 * ->next == @clientptr.
	 */
	struct e_client *next = atomic_load_explicit(
			&c->next, memory_order_relaxed),
		*old = atomic_exchange(pp, next);
	assert(old == c);

	/* however, there's some fixups that gotta get perfermt. for one, c->next
	 * may have changed, so we might have written the wrong thing. this'd keep
	 * it visible past death, which is bad. the fix is iterative.
	 */
	struct e_client *next2;
	while(next != (next2 = atomic_load(&c->next))) {
		struct e_client *old2 = atomic_exchange(pp, next2);
		assert(old2 != c);
		next = next2;
	}

	/* the other fixup is for the parent having removed itself, making `pp'
	 * unreachable through the list, thereby making our exchange entirely
	 * ineffective. the fix is leaving the dead `pp' lie and starting over.
	 */
	if(pp != &client_list) {
		bool found = false;
		for(struct e_client *cur = atomic_load(&client_list);
			cur != NULL;
			cur = atomic_load(&cur->next))
		{
			if(pp == &cur->next) {
				found = true;
				break;
			}
		}
		if(!found) client_dtor(clientptr);
	}
}


static void client_ctor(struct e_client *c)
{
	assert(!c->initialized);
	c->initialized = true;
	assert(c->epoch == 0);
	c->next = atomic_load_explicit(&client_list, memory_order_relaxed);
	while(!atomic_compare_exchange_strong_explicit(&client_list,
		&c->next, c, memory_order_release, memory_order_relaxed))
	{
		/* spin */
	}
}


static struct e_client *get_client(void)
{
	void *ptr = e_ext_get(sizeof(struct e_client), &client_dtor);
	assert(ptr != NULL);
	struct e_client *c = ptr;
	if(!c->initialized) {
		client_ctor(c);
		assert(c->initialized);
	}
	return c;
}


static inline unsigned long next_epoch(unsigned long e) {
	return e != ULONG_MAX ? e + 1 : 2;
}


/* advance epoch & call the previously-quiet dtors. */
static void tick(unsigned long old_epoch)
{
	assert(e_inside());		/* prevent further ticks */

	/* bump the epoch number. */
	unsigned long new_epoch = next_epoch(old_epoch);
	if(!atomic_compare_exchange_weak_explicit(&global_epoch,
		&old_epoch, new_epoch, memory_order_release, memory_order_relaxed))
	{
		/* we're not the ones that ticked it forward. */
		assert(old_epoch == new_epoch);
		return;
	}

	struct e_dtor_call *dead = atomic_exchange_explicit(
		&epoch_dtors[(old_epoch - 2) & 3], NULL,
		memory_order_relaxed);

	/* call the list in push order, i.e. reverse it first. */
	struct e_dtor_call *head = NULL;
	while(dead != NULL) {
		struct e_dtor_call *next = dead->next;
		dead->next = head;
		head = dead;
		dead = next;
	}
	while(head != NULL) {
		(*head->dtor_fn)(head->ptr);
		struct e_dtor_call *next = head->next;
		free(head);
		head = next;
	}
}


static void maybe_tick(unsigned long epoch, struct e_client *self)
{
	assert(e_inside());
	struct e_client *c = atomic_load_explicit(&client_list,
		memory_order_consume);
	bool quiet = true;
	while(c != NULL) {
		unsigned long c_epoch = atomic_load_explicit(&c->epoch,
			memory_order_relaxed);
		if(c != self && c_epoch > 0 && c_epoch < epoch) {
			quiet = false;
			break;
		}
		c = atomic_load_explicit(&c->next, memory_order_consume);
	}
	if(quiet) tick(epoch);
}


/* TODO: e_begin() and e_end() don't enforce matching cookies under !NDEBUG.
 * they should.
 */
int e_begin(void)
{
	struct e_client *c = get_client();
	if(c->epoch > 0) return 0;	/* inner begin; disregard. */

	unsigned long epoch = atomic_load_explicit(&global_epoch,
		memory_order_consume);
	atomic_store_explicit(&c->epoch, epoch, memory_order_release);
	return 1;
}


void e_end(int cookie)
{
	struct e_client *c = get_client();
	if(cookie == 0) return;

	unsigned long epoch = atomic_load_explicit(&global_epoch,
			memory_order_consume),
		count = atomic_load_explicit(&epoch_counts[epoch & 3],
			memory_order_relaxed) + atomic_load_explicit(
				&epoch_counts[(epoch - 1) & 3], memory_order_relaxed);
	assert(epoch == c->epoch || epoch == next_epoch(c->epoch));
	/* FIXME: come up with some other policy for making the clock tick. it's
	 * worthwhile for avoiding a loop through every client.
	 */
	if(count > 0) maybe_tick(epoch, c);

	atomic_store_explicit(&c->epoch, 0, memory_order_release);
}


int e_torpor(void)
{
	if(!e_inside()) return -EINVAL;
	return 2;
}


int e_rouse(int cookie)
{
	return 0;
}


bool e_inside(void)
{
	struct e_client *c = get_client();
	return c->epoch > 0;
}


void _e_call_dtor(void (*dtor_fn)(void *ptr), void *ptr)
{
	/* TODO: use an aligned-chunk allocation scheme for these, since each is
	 * supposed to be quite small.
	 */
	struct e_dtor_call *call = malloc(sizeof(*call));
	call->dtor_fn = dtor_fn;
	call->ptr = ptr;
	unsigned long epoch = atomic_load_explicit(&global_epoch,
		memory_order_consume);
	struct e_dtor_call *_Atomic *head = &epoch_dtors[epoch & 3];
	do {
		call->next = atomic_load_explicit(head, memory_order_relaxed);
	} while(!atomic_compare_exchange_strong(head, &call->next, call));
	atomic_fetch_add_explicit(&epoch_counts[epoch & 3], 1,
		memory_order_release);
}


void e_free(void *ptr) {
	e_call_dtor(&free, ptr);
}
