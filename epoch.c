
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
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


static struct e_client *_Atomic client_list = NULL;


static void client_dtor(void *clientptr)
{
	/* delink from client_list */
	struct e_client *c = clientptr, *prev,
		*_Atomic *pp = &client_list;
	do {
		prev = atomic_load(pp);
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
		c->next = atomic_load(&client_list);
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


int e_begin(void)
{
	struct e_client *c = get_client();
	if(c->epoch == 0) {
		c->epoch = 1;
		return 1;
	} else {
		return 0;
	}
}


void e_end(int cookie)
{
	struct e_client *c = get_client();
	if(cookie == 1 && c->epoch == 1) c->epoch = 0;
	else assert(cookie == 0);
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
}


void e_free(void *ptr) {
	e_call_dtor(&free, ptr);
}
