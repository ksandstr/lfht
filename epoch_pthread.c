
#include <stdlib.h>
#include <pthread.h>

#include "epoch.h"


static pthread_key_t epoch_key;
static void (*epoch_dtor_fn)(void *ptr) = NULL;


static void call_dtor(void *ptr) {
	(*epoch_dtor_fn)(ptr);
}


static void initialize_pthread_epoch_client(void)
{
	epoch_key = pthread_key_create(&epoch_key, &call_dtor);
}


void *e_ext_get(size_t size, void (*dtor_fn)(void *ptr))
{
	/* there may be a slicker, smoother way to do this that doesn't involve
	 * always calling pthread_once(), but alas, this is proof-of-concept code
	 * and can't afford fancy things like that.
	 */
	static pthread_once_t init_once = PTHREAD_ONCE_INIT;
	pthread_once(&init_once, &initialize_pthread_epoch_client);

	void *ptr = pthread_getspecific(epoch_key);
	if(ptr == NULL) {
		if(epoch_dtor_fn == NULL) epoch_dtor_fn = dtor_fn;
		ptr = calloc(1, size);
		pthread_setspecific(epoch_key, ptr);
	}

	return ptr;
}
