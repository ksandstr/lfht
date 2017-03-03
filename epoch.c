
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
	_Atomic unsigned long epoch;	/* 0 for outside. */

	/* constants after ctor */
	bool initialized;
};


struct e_dtor_call
{
	struct e_dtor_call *_Atomic next;
	void (*dtor_fn)(void *ptr);
	void *ptr;
};


static struct e_client *_Atomic client_list = NULL;

static _Atomic unsigned long global_epoch = 1;
static volatile atomic_flag global_epoch_lock = ATOMIC_FLAG_INIT;

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
	/* delink from client_list */
	struct e_client *c = clientptr, *prev,
		*_Atomic *pp = &client_list;
	do {
		prev = atomic_load_explicit(pp, memory_order_relaxed);
		if(prev == c) break;
		pp = &prev->next;
	} while(prev != NULL);

	struct e_client *old = c;
	if(!atomic_compare_exchange_strong(pp, &old, c->next)) {
		/* bah! concurrent client exit happened. redo from start. */
		client_dtor(clientptr);
		return;
	}

	/* dispose safely. */
	e_free(c);
}


static void client_ctor(struct e_client *c)
{
	assert(!c->initialized);
	c->initialized = true;
	do {
		c->next = atomic_load_explicit(&client_list, memory_order_relaxed);
	} while(!atomic_compare_exchange_weak(&client_list, &c->next, c));
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


/* advance epoch & call the previously-quiet dtors. */
static void tick(unsigned long old_epoch)
{
	/* get the lock or go away. */
	if(atomic_flag_test_and_set(&global_epoch_lock)) {
		/* true = was already locked */
		return;
	}

	/* bump the epoch number. */
	unsigned long new_epoch = old_epoch + 1;
	if(old_epoch == ULONG_MAX) new_epoch = 2;
	if(!atomic_compare_exchange_strong(&global_epoch,
		&old_epoch, new_epoch))
	{
		/* nuh-uh! */
		atomic_flag_clear_explicit(&global_epoch_lock, memory_order_release);
		return;
	}

	struct e_dtor_call *dead = atomic_exchange_explicit(
		&epoch_dtors[(old_epoch - 2) & 3], NULL,
		memory_order_relaxed);
	atomic_flag_clear_explicit(&global_epoch_lock, memory_order_release);

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
		if(c_epoch > 0 && c != self) quiet = false;
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
