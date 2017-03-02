
#include <stdlib.h>
#include <stdbool.h>

#include "epoch.h"


int e_begin(void)
{
	return 0;
}


void e_end(int cookie)
{
}


int e_torpor(void)
{
	return 2;
}


int e_rouse(int cookie)
{
	return 0;
}


bool e_inside(void)
{
	return true;
}


void _e_call_dtor(void (*dtor_fn)(void *ptr), void *ptr)
{
}


void e_free(void *ptr) {
	e_call_dtor(&free, ptr);
}
