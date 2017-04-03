
/* convenient access to data items split per CPU, or rather, per L1d cache.
 * for example, counters in lock-free data structures which'd otherwise
 * present a greater bottleneck.
 */

#ifndef PERCPU_H
#define PERCPU_H

#include <stdlib.h>
#include <assert.h>
#include <sched.h>


struct percpu
{
	int n_buckets, shift;
	void *buckets[];
};


extern struct percpu *percpu_new(
	size_t bucket_size,
	void (*init_fn)(void *bucketptr));

static inline void percpu_free(struct percpu *p) {
	free(p);
}

static inline void *percpu_get(struct percpu *p, int b_ix) {
	assert(b_ix < p->n_buckets);
	return p->buckets[b_ix];
}

static inline void *percpu_my(struct percpu *p) {
	return percpu_get(p, sched_getcpu() >> p->shift);
}

#endif
