
#ifndef EPOCH_H
#define EPOCH_H

#include <stdlib.h>
#include <stdbool.h>
#include <ccan/typesafe_cb/typesafe_cb.h>


/* start or end an epoch bracket. recursive; the protected period ends with
 * the call to the outermost e_end(). cookies are used to enforce begin-end
 * ordering in debug builds.
 *
 * pointers released using e_free() or e_call_dtor() remain valid for as long
 * as # of calls to e_begin() > # of calls to e_end(), per thread. such
 * pointers must not be carried over an end-begin bracket unless an outer
 * bracket exists.
 */
extern int e_begin(void);
extern void e_end(int cookie);

/* to sleep in an epoch bracket without delaying memory reclamation, a thread
 * may enter a "break permitted" period and check its status afterward. if the
 * thread's epoch bracket was interrupted, it should immediately break out
 * into the outermost e_end() and restart. epoch-protected pointers will be
 * invalid during the period, only becoming valid again if the status check
 * didn't indicate restart.
 */
extern int e_torpor(void);				/* returns a cookie, or -EINVAL */
extern int e_rouse(int torpor_cookie);	/* 0 for ok, -EAGAIN for restart */

/* library code may assert() this, or against it to mark a definite restart
 * point.
 */
extern bool e_inside(void);

/* it makes no proper sense to call these outside an epoch bracket, so don't
 * do that.
 */
#define e_call_dtor(fn, ptr) \
	_e_call_dtor(typesafe_cb(void, void *, (fn), (ptr)), (ptr))
extern void _e_call_dtor(void (*dtor_fn)(void *ptr), void *ptr);

/* wrapper for free(3). */
extern void e_free(void *ptr);


/* interface to the C runtime: returns a per-thread free(3)able block of @size
 * bytes, initialized to all zeroes and released with a call to @dtor at
 * thread exit.
 *
 * default implementation in epoch_pthread.c uses pthread TLS, but things like
 * OS kernels can link in a per-CPU setup instead (and never call @dtor).
 */
extern void *e_ext_get(size_t size, void (*dtor_fn)(void *ptr));


#endif
