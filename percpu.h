/* convenient access to data items split per CPU (D1$). */
#ifndef _PERCPU_H
#define _PERCPU_H

#include <sched.h>

struct percpu {
	int n_buckets, shift;
	void *buckets[];
};

extern struct percpu *percpu_new(size_t bucket_size, void (*init_fn)(void *bucketptr));
extern void percpu_free(struct percpu *p);

static inline void *percpu_get(struct percpu *p, int b_ix) { return p->buckets[b_ix]; }
static inline void *percpu_my(struct percpu *p) { return percpu_get(p, sched_getcpu() >> p->shift); }

#endif
