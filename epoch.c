
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <limits.h>
#include <assert.h>
#include <unistd.h>
#include <sched.h>
#include <errno.h>

#include <ccan/likely/likely.h>
#include <ccan/container_of/container_of.h>

#include "nbsl.h"
#include "percpu.h"
#include "epoch.h"


struct e_client
{
	/* thread private */
	size_t count_since_tick;
	bool initialized;

	/* concurrent access */
	struct nbsl_node link __attribute__((aligned(64)));
	_Atomic unsigned long epoch;	/* valid iff active > 0. */
	_Atomic int active;				/* 0 for idle */
};


struct e_dtor_call
{
	struct e_dtor_call *next;
	void (*dtor_fn)(void *ptr);
	void *ptr;
};


/* cpu-split bucket for dtors and dtor counts, per epoch.
 *
 * [epoch + 1 mod 4] = NULL
 * [epoch     mod 4] = fresh dtors, current insert
 * [epoch - 1 mod 4] = quiet dtors, possibly under access, late insert
 * [epoch - 2 mod 4] = in-progress dtors (then NULL)
 */
struct e_bucket {
	_Atomic int lock;	/* per-cpu, cheap and benign */
	struct e_dtor_call *dtor_list[4];
	unsigned count[4];
} __attribute__((aligned(64)));

#define GET_BUCKET() ((struct e_bucket *)percpu_my(epoch_pc))


static _Atomic unsigned long global_epoch = 2;
static struct percpu *_Atomic epoch_pc = NULL;
static struct nbsl client_list = NBSL_LIST_INIT(client_list);
static _Atomic bool global_init_flag = false;


static void e_bucket_ctor(void *ptr)
{
	struct e_bucket *b = ptr;
	*b = (struct e_bucket){ .lock = 0 };
	atomic_thread_fence(memory_order_release);
}


static void ensure_global_init(void)
{
	struct percpu *pc = percpu_new(sizeof(struct e_bucket), &e_bucket_ctor);
	if(pc == NULL) {
		if(atomic_load(&epoch_pc) == NULL) {
			fprintf(stderr, "epoch: %s: out of memory!\n", __func__);
			abort();
		} else {
			/* huh, who'd've thunk. */
			return;
		}
	}

	struct percpu *old = NULL;
	if(!atomic_compare_exchange_strong_explicit(&epoch_pc, &old, pc,
		memory_order_release, memory_order_relaxed))
	{
		assert(old != NULL);
		percpu_free(pc);
	}

	atomic_store_explicit(&global_init_flag, true, memory_order_release);
}


static void client_dtor(void *clientptr)
{
	struct e_client *c = clientptr;
	bool ok = nbsl_del(&client_list, &c->link);
	if(!ok) {
		fprintf(stderr, "epoch: %s: didn't delete c=%p??\n", __func__, c);
		abort();
	}
}


static void client_ctor(struct e_client *c)
{
	assert(!c->initialized);
	c->initialized = true;
	assert(c->epoch == 0);
	assert(c->count_since_tick == 0);

	while(!nbsl_push(&client_list, nbsl_top(&client_list), &c->link)) {
		/* spin */
	}
}


static struct e_client *get_client(void)
{
	void *ptr = e_ext_get(sizeof(struct e_client), &client_dtor);
	assert(ptr != NULL);
	struct e_client *c = ptr;
	if(!c->initialized) {
		if(!atomic_load(&global_init_flag)) ensure_global_init();

		client_ctor(c);
		assert(c->initialized);
	}
	return c;
}


static void lock_bucket(struct e_bucket *bk)
{
	int old = 0;
	while(!atomic_compare_exchange_weak_explicit(&bk->lock, &old, 1,
		memory_order_acquire, memory_order_relaxed))
	{
		/* buckets are mostly per-cpu so there's no point to spinning; most
		 * likely we're here because another thread on this CPU was scheduled
		 * off (and will return sooner this way).
		 */
		sched_yield();
		old = 0;
	}
}


static void unlock_bucket(struct e_bucket *bk) {
	int old = atomic_exchange_explicit(&bk->lock, 0, memory_order_release);
	assert(old == 1);
}


static inline unsigned long next_epoch(unsigned long e) {
	return e != ULONG_MAX ? e + 1 : 2;
}


/* advance epoch & call the previously-quiet dtors. */
static void tick(unsigned long old_epoch)
{
	assert(e_inside());		/* prevent further ticks */

	/* bump the epoch number, caring nothing for the result. */
	unsigned long oldval = old_epoch, new_epoch = next_epoch(old_epoch);
	atomic_compare_exchange_strong_explicit(&global_epoch,
		&oldval, new_epoch, memory_order_release, memory_order_relaxed);

	/* starting from our own CPU, do each dtor list in turn. */
	int gone = (old_epoch - 2) & 3;
	for(int base = sched_getcpu() >> epoch_pc->shift, i = 0;
		i < epoch_pc->n_buckets;
		i++)
	{
		/* the ordering is important here; the global epoch must advance
		 * before bucket locks are taken. this makes e_call_dtor() valid.
		 */
		struct e_bucket *bk = percpu_get(epoch_pc, base ^ i);
		lock_bucket(bk);
		bk->count[gone] = 0;
		struct e_dtor_call *dead = bk->dtor_list[gone];
		bk->dtor_list[gone] = NULL;
		unlock_bucket(bk);

		/* call the list in push order, i.e. reverse it first. */
		struct e_dtor_call *head = NULL;
		while(dead != NULL) {
			struct e_dtor_call *next = dead->next;
			dead->next = head;
			head = dead;	/* saturday mornings, man */
			dead = next;
		}
		while(head != NULL) {
			(*head->dtor_fn)(head->ptr);
			struct e_dtor_call *next = head->next;
			free(head);
			head = next;
		}
	}
}


static void maybe_tick(unsigned long epoch, struct e_client *self)
{
	assert(e_inside());

	bool quiet = true;
	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(&client_list, &it);
		cur != NULL;
		cur = nbsl_next(&client_list, &it))
	{
		struct e_client *c = container_of(cur, struct e_client, link);
		int c_active = atomic_load(&c->active);
		unsigned long c_epoch = atomic_load(&c->epoch);
		if(c != self && c_active > 0 && c_epoch < epoch) {
			quiet = false;
			break;
		}
	}
	if(quiet) {
		tick(epoch);
		self->count_since_tick = 0;
	}
}


static inline int make_cookie(unsigned long epoch, bool nested) {
	/* avoid overflowing a signed int. */
	return ((epoch & 0x3fffffff) << 1) | (nested ? 1 : 0);
}


/* TODO: e_begin() and e_end() don't enforce matching cookies under !NDEBUG.
 * they should.
 */
int e_begin(void)
{
	struct e_client *c = get_client();
	unsigned long epoch;
	bool nested;
	if(atomic_fetch_add(&c->active, 1) > 0) {
		nested = true;
		epoch = atomic_load_explicit(&c->epoch, memory_order_relaxed);
	} else {
		nested = false;
		/* has to happen in a loop. fortunately this is completely valid. the
		 * "active" flag up there ensures that this terminates at the latest
		 * when all other threads in the system have ticked the epoch twice,
		 * which is better than the indefinite live-locked spinning that'd
		 * happen without.
		 */
		epoch = 0;
		do {
			epoch = atomic_load(&global_epoch);
			atomic_store(&c->epoch, epoch);
		} while(epoch != atomic_load(&global_epoch));
	}

	return make_cookie(epoch, nested);
}


static size_t sum_counts(int e)
{
	size_t sum = 0;
	for(int b = sched_getcpu() >> epoch_pc->shift, i = 0;
		i < epoch_pc->n_buckets;
		i++)
	{
		struct e_bucket *bk = percpu_get(epoch_pc, b ^ i);
		sum += atomic_load_explicit(&bk->count[e], memory_order_relaxed);
	}
	return sum;
}


void e_end(int cookie)
{
	struct e_client *c = get_client();
	int old_active = atomic_load_explicit(&c->active, memory_order_relaxed);
	assert(old_active > 0);
	if(old_active == 1) {
		/* try to tick forward only if the counts say so. examine all counts
		 * every 16 brackets, resetting at tick.
		 */
		bool deep = (++c->count_since_tick & 0x1f) == 0;
		unsigned long epoch = atomic_load_explicit(&global_epoch,
			memory_order_acquire);
		assert(epoch == c->epoch || epoch == next_epoch(c->epoch));
		if(GET_BUCKET()->count[epoch & 3] > 0
			|| (deep && sum_counts(epoch & 3) > 0))
		{
			maybe_tick(epoch, c);
		}
	}
	old_active = atomic_fetch_sub(&c->active, 1);
	assert(old_active > 0);
	assert(old_active > 1 || (cookie & 1) == 0);
}


int e_resume(int cookie)
{
	unsigned long epoch = atomic_load_explicit(&global_epoch,
		memory_order_relaxed);
	if(cookie >> 1 != (epoch & 0x3fffffff)) return -EBUSY;

	struct e_client *c = get_client();
	bool nested = atomic_fetch_add(&c->active, 1) > 0;
	atomic_store(&c->epoch, epoch);
	if(!nested && atomic_load(&global_epoch) != epoch) {
		/* there was a tick in between, so ours didn't take. */
		atomic_fetch_sub(&c->active, 1);
		return -EBUSY;
	} else {
		return make_cookie(epoch, nested);
	}
}


bool e_inside(void)
{
	struct e_client *c = get_client();
	return atomic_load_explicit(&c->active, memory_order_relaxed) > 0;
}


void _e_call_dtor(void (*dtor_fn)(void *ptr), void *ptr)
{
	if(unlikely(!atomic_load_explicit(&global_init_flag,
		memory_order_relaxed)))
	{
		ensure_global_init();
	}

	/* TODO: use an aligned-chunk allocation scheme for these, since each is
	 * supposed to be quite small.
	 */
	struct e_dtor_call *call = malloc(sizeof *call);
	call->dtor_fn = dtor_fn;
	call->ptr = ptr;

	/* the ordering is important; the dtor producer must read epoch while
	 * holding a bucket lock.
	 *
	 * this exploits an interaction via the lock where tick() inhibits a
	 * further epoch tick by spinning, which implies that while a bucket lock
	 * is held the epoch value will stay at global_epoch or global_epoch - 1,
	 * which are valid for dtor deposits.
	 */
	struct e_bucket *bk = GET_BUCKET();
	lock_bucket(bk);
	unsigned long epoch = atomic_load_explicit(&global_epoch,
		memory_order_acquire);
	struct e_dtor_call **head = &bk->dtor_list[epoch & 3];
	call->next = *head;
	*head = call;
	bk->count[epoch & 3]++;
	assert(epoch >= atomic_load(&global_epoch) - 1);
	unlock_bucket(bk);
}


void e_free(void *ptr) {
	e_call_dtor(&free, ptr);
}
