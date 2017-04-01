
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
#include "epoch.h"


#define GET_BUCKET() (&buckets[sched_getcpu() >> bucket_shift])


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
	struct e_dtor_call *_Atomic next;
	void (*dtor_fn)(void *ptr);
	void *ptr;
};


/* cpu-split bucket for dtors and dtor counts, per epoch.
 *
 * [epoch + 1 mod 4] = NULL
 * [epoch     mod 4] = fresh dtors, current insert position
 * [epoch - 1 mod 4] = quiet dtors, possibly still under access
 * [epoch - 2 mod 4] = in-progress dtors (then NULL)
 */
struct e_bucket {
	struct e_dtor_call *_Atomic dtor_list[4];
	_Atomic unsigned long count[4];
} __attribute__((aligned(64)));


static struct nbsl client_list = NBSL_LIST_INIT(client_list);

static _Atomic unsigned long global_epoch = 2;

static struct e_bucket *buckets = NULL;
static int num_buckets = 0, bucket_shift = 0;

static _Atomic bool global_init_flag = false;


static void ensure_global_init(void)
{
	/* try to figure out the proper setup. idea here is that from 8 threads
	 * up, the system is likely to share highest-level caches between two
	 * sibling CPUs. this is a total fudge, but until GUHNOO/Lynnocks starts
	 * handing 'em out nicely, it'll do.
	 */
	int n_cpus = sysconf(_SC_NPROCESSORS_ONLN), zero = 0,
		shift = n_cpus >= 8 ? 1 : 0;
	if(!atomic_compare_exchange_strong(&bucket_shift, &zero, shift)) {
		shift = zero;
		zero = 0;
	}
	if(!atomic_compare_exchange_strong(&num_buckets,
		&zero, n_cpus >> shift))
	{
		n_cpus = zero << shift;
		zero = 0;
	}

	struct e_bucket *bks = aligned_alloc(alignof(*bks),
		sizeof(*bks) * (n_cpus >> shift));
	if(bks == NULL) {
		fprintf(stderr, "epoch: %s: out of memory!\n", __func__);
		abort();
	}
	for(int i=0; i < n_cpus >> shift; i++) {
		for(int j=0; j < 4; j++) {
			atomic_store_explicit(&bks[i].dtor_list[j], NULL,
				memory_order_relaxed);
			atomic_store_explicit(&bks[i].count[j], 0, memory_order_relaxed);
		}
	}
	struct e_bucket *old = NULL;
	if(!atomic_compare_exchange_strong_explicit(&buckets, &old, bks,
		memory_order_release, memory_order_relaxed))
	{
		assert(old != NULL);
		free(bks);
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
	for(int base = sched_getcpu() >> bucket_shift, i = 0;
		i < num_buckets;
		i++)
	{
		/* once-only is guaranteed by atomic exchange. */
		struct e_bucket *bk = &buckets[base ^ i];
		atomic_store_explicit(&bk->count[(old_epoch - 2) & 3], 0,
			memory_order_relaxed);
		struct e_dtor_call *dead = atomic_exchange_explicit(
			&bk->dtor_list[(old_epoch - 2) & 3], NULL,
			memory_order_acq_rel);
		if(dead == NULL) continue;

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


/* TODO: e_begin() and e_end() don't enforce matching cookies under !NDEBUG.
 * they should.
 */
int e_begin(void)
{
	struct e_client *c = get_client();
	if(atomic_fetch_add(&c->active, 1) > 0) {
		/* nested */
		return 0;
	}

	/* has to happen in a loop. fortunately this is completely valid. the
	 * "active" flag up there ensures that this terminates at the latest when
	 * all other threads in the system have ticked the epoch twice, which is
	 * better than the indefinite live-locked spinning that'd happen without.
	 */
	unsigned long epoch = 0;
	do {
		epoch = atomic_load(&global_epoch);
		atomic_store(&c->epoch, epoch);
	} while(epoch != atomic_load(&global_epoch));
	return 1;
}


static size_t sum_counts(int e)
{
	size_t sum = 0;
	for(int b = sched_getcpu() >> bucket_shift, i=0; i < num_buckets; i++) {
		sum += atomic_load_explicit(&buckets[b ^ i].count[e],
			memory_order_relaxed);
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
			memory_order_consume);
		assert(epoch == c->epoch || epoch == next_epoch(c->epoch));
		if(GET_BUCKET()->count[epoch & 3] > 0
			|| (deep && sum_counts(epoch & 3) > 0))
		{
			maybe_tick(epoch, c);
		}
	}
	old_active = atomic_fetch_sub(&c->active, 1);
	assert(old_active > 0);
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
	struct e_dtor_call *call = malloc(sizeof(*call));
	call->dtor_fn = dtor_fn;
	call->ptr = ptr;
	unsigned long epoch = atomic_load_explicit(&global_epoch,
		memory_order_consume);
	struct e_bucket *bk = GET_BUCKET();
	struct e_dtor_call *_Atomic *head = &bk->dtor_list[epoch & 3];
	do {
		call->next = atomic_load_explicit(head, memory_order_relaxed);
	} while(!atomic_compare_exchange_weak(head, &call->next, call));
	atomic_fetch_add_explicit(&bk->count[epoch & 3], 1,
		memory_order_release);
}


void e_free(void *ptr) {
	e_call_dtor(&free, ptr);
}
