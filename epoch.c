#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <limits.h>
#include <assert.h>
#include <threads.h>
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
	/* concurrent access */
	struct nbsl_node link __attribute__((aligned(64)));
	_Atomic unsigned long epoch;	/* valid iff active > 0. */
	_Atomic int active;				/* 0 for idle */
};

struct e_dtor_call {
	struct e_dtor_call *next;
	void (*dtor_fn)(void *ptr);
	void *ptr;
};

/* cpu-split bucket for dtors and dtor counts, per epoch.
 * [epoch + 1 mod 4] = NULL
 * [epoch     mod 4] = fresh dtors, current insert
 * [epoch - 1 mod 4] = quiet dtors, possibly under access, late insert
 * [epoch - 2 mod 4] = in-progress dtors (then NULL)
 */
struct e_bucket {
	struct e_dtor_call *_Atomic dtor_list[4];
	_Atomic unsigned count[4];
} __attribute__((aligned(64)));

static _Atomic unsigned long global_epoch = 2;
static struct percpu *epoch_pc = NULL;
static struct nbsl client_list = NBSL_LIST_INIT(client_list);
static once_flag epoch_init_once = ONCE_FLAG_INIT;
static tss_t client_key;

static struct e_bucket *my_bucket(void) { return percpu_my(epoch_pc); }

static void bucket_ctor(void *ptr) { *(struct e_bucket *)ptr = (struct e_bucket){ }; }

static void client_dtor(void *priv) {
	struct e_client *c = priv;
	assert(c->active == 0);
	if(!nbsl_del(&client_list, &c->link)) abort();
}

static void epoch_init(void) {
	if(tss_create(&client_key, &client_dtor) != thrd_success) abort();
	epoch_pc = percpu_new(sizeof(struct e_bucket), &bucket_ctor);
	if(epoch_pc == NULL) abort(); /* gcc? */
	atomic_thread_fence(memory_order_release);
}

static struct e_client *get_client(void)
{
	call_once(&epoch_init_once, &epoch_init);
	struct e_client *c = tss_get(client_key);
	if(unlikely(c == NULL)) {
		if(c = malloc(sizeof *c), c == NULL) abort();
		*c = (struct e_client){ };
		while(!nbsl_push(&client_list, nbsl_top(&client_list), &c->link)) /* spin */ ;
		tss_set(client_key, c);
	}
	return c;
}

static unsigned long next_epoch(unsigned long e) { return e < ULONG_MAX ? e + 1 : 2; }

/* advance epoch, call quieted dtors */
static void tick(unsigned long old_epoch)
{
	unsigned long oldval = old_epoch, new_epoch = next_epoch(old_epoch);
	atomic_compare_exchange_strong_explicit(&global_epoch, &oldval, new_epoch, memory_order_release, memory_order_relaxed);
	int gone = (old_epoch - 2) & 3;
	for(int i = 0, base = sched_getcpu() >> epoch_pc->shift; i < epoch_pc->n_buckets; i++) {
		struct e_bucket *bk = percpu_get(epoch_pc, base ^ i);
		struct e_dtor_call *dead = atomic_exchange_explicit(&bk->dtor_list[gone], NULL, memory_order_acquire);
		unsigned down = 0;
		/* call the list in push order, i.e. reverse it first. */
		struct e_dtor_call *head = NULL;
		while(dead != NULL) {
			struct e_dtor_call *next = dead->next;
			dead->next = head;
			head = dead; /* saturday mornings, man */
			dead = next;
		}
		while(head != NULL) {
			(*head->dtor_fn)(head->ptr);
			struct e_dtor_call *next = head->next;
			free(head);
			head = next;
			down++;
		}
		atomic_fetch_sub_explicit(&bk->count[gone], down, memory_order_release);
	}
}

static inline int make_cookie(unsigned long epoch, bool nested) {
	/* avoid overflowing a signed int. */
	return ((epoch & 0x3fffffff) << 1) | (nested ? 1 : 0);
}

int e_begin(void)
{
	struct e_client *c = get_client();
	bool nested = atomic_fetch_add_explicit(&c->active, 1, memory_order_acquire) > 0;
	if(!nested) atomic_store_explicit(&c->epoch, atomic_load_explicit(&global_epoch, memory_order_acquire), memory_order_release);
	return make_cookie(atomic_load_explicit(&c->epoch, memory_order_relaxed), nested);
}

bool e_inside(void) { return atomic_load_explicit(&get_client()->active, memory_order_relaxed) > 0; }

static void maybe_tick(unsigned long epoch, struct e_client *self)
{
	assert(e_inside());
	struct nbsl_iter it;
	for(struct nbsl_node *cur = nbsl_first(&client_list, &it); cur != NULL; cur = nbsl_next(&client_list, &it)) {
		struct e_client *c = container_of(cur, struct e_client, link);
		if(c != self && c->active > 0 && c->epoch < epoch) return; /* not quiet; slew tolerated. */
	}
	tick(epoch);
	self->count_since_tick = 0;
}

static size_t sum_counts(int e)
{
	size_t sum = 0;
	for(int b = sched_getcpu() >> epoch_pc->shift, i = 0; i < epoch_pc->n_buckets; i++) {
		struct e_bucket *bk = percpu_get(epoch_pc, b ^ i);
		sum += atomic_load_explicit(&bk->count[e], memory_order_relaxed);
	}
	return sum;
}

/* TODO: enforce matching cookies under !NDEBUG */
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
		unsigned long epoch = atomic_load_explicit(&global_epoch, memory_order_acquire);
		assert(epoch == c->epoch || epoch == next_epoch(c->epoch));
		if(my_bucket()->count[epoch & 3] > 0 || (deep && sum_counts(epoch & 3) > 0)) maybe_tick(epoch, c);
	}
	old_active = atomic_fetch_sub_explicit(&c->active, 1, memory_order_release);
	assert(old_active > 0 && (old_active > 1 || (~cookie & 1)));
}

int e_resume(int cookie)
{
	unsigned long epoch = atomic_load_explicit(&global_epoch, memory_order_relaxed);
	if(cookie >> 1 != (epoch & 0x3fffffff)) return -EBUSY;
	struct e_client *c = get_client();
	bool nested = atomic_fetch_add_explicit(&c->active, 1, memory_order_release) > 0;
	atomic_store_explicit(&c->epoch, epoch, memory_order_release);
	if(global_epoch == epoch || nested) return make_cookie(epoch, nested);
	else {
		/* there was a tick in between, so ours didn't take. */
		atomic_fetch_sub(&c->active, 1);
		return -EBUSY;
	}
}

void _e_call_dtor(void (*dtor_fn)(void *ptr), void *ptr)
{
	if(unlikely(epoch_pc == NULL)) call_once(&epoch_init_once, &epoch_init);
	struct e_dtor_call *call = malloc(sizeof *call);
	if(call == NULL) abort();
	*call = (struct e_dtor_call){ .dtor_fn = dtor_fn, .ptr = ptr };
	struct e_bucket *bk = my_bucket();
	unsigned long epoch = atomic_load_explicit(&global_epoch, memory_order_relaxed);
	atomic_fetch_add_explicit(&bk->count[epoch & 3], 1, memory_order_relaxed);
	call->next = atomic_load_explicit(&bk->dtor_list[epoch & 3], memory_order_acquire);
	while(!atomic_compare_exchange_strong_explicit(&bk->dtor_list[epoch & 3], &call->next, call, memory_order_release, memory_order_relaxed)) /* repeat */ ;
	assert(epoch >= global_epoch - 1);
}

void e_free(void *ptr) { e_call_dtor(&free, ptr); }
