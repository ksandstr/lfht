
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#include <unistd.h>

#include "percpu.h"


struct percpu *percpu_new(
	size_t bucket_size, void (*init_fn)(void *ptr))
{
	/* try to figure out the proper setup. idea here is that from 8 threads
	 * up, the system is likely to share highest-level caches between two
	 * sibling CPUs. this is a total fudge, but until GUHNOO/Lynnocks starts
	 * handing 'em out nicely, it'll do.
	 */
	int n_cpus = sysconf(_SC_NPROCESSORS_ONLN),
		shift = n_cpus >= 8 ? 1 : 0,
		n_buckets = n_cpus >> shift;
	bucket_size = (bucket_size + 63) & ~63;
	size_t base_size = sizeof(struct percpu) + sizeof(void *) * n_buckets;
	base_size = (base_size + 63) & ~63;
	struct percpu *p = aligned_alloc(64, base_size + bucket_size * n_buckets);
	if(p == NULL) return NULL;

	p->n_buckets = n_buckets;
	p->shift = shift;
	for(int i=0; i < n_buckets; i++) {
		p->buckets[i] = (void *)p + base_size + bucket_size * i;
		memset(p->buckets[i], '\0', bucket_size);
		if(init_fn != NULL) (*init_fn)(p->buckets[i]);
	}

	atomic_thread_fence(memory_order_release);

	return p;
}
