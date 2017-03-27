
What
====

This directory contains an implementation of a lock-free wait-free
singly-linked lists ("nbsl"), a lock-free hashtable ("lfht"), an epoch
reclamation mechanism, and some tests for each.

Licensed under the GNU GPL v3-or-later, found in the COPYING file in this
directory.


!! Warning !! Danger !! Warning !!
----------------------------------

These data structures are not proven to work. Lock-free and wait-free
algorithms, as well as any application using them, are fraught with subtle
breakage; as such, users of this code should do their own review and proceed
at their own risk. Caveat lector to the max: this'll bite your dog's dick off.


Design
------

`lfht` is based on a design someone discussed on Reddit a few years ago in the
context of real-time data structures. Fundamentally the idea is simple: when a
hash table becomes full, instead of allocating a new table and migrating all
entries over in a big O(n) loop, allocate a new table and migrate entries one
at a time in the `add` operation. This ensures that the old table will be
cleared before the new one fills up, and makes `add`'s maximum latency mostly
consist of `calloc()` execution.

This table starts out from the basic design of CCAN's htable module by Rusty
Russell, and modifies it to accommodate lookups from multiple tables at the
same time and inter-table migration. The twin optimizations of perfect-bit and
common-bits are retained by creating a new table with an updated mask whenever
a conflicting entry would've been added; rehashing when there are too many
deleted items is achieved in the same way.

Lock-free implementation follows trivially. This design has not been analyzed
for wait-freeness, but it's likely that it won't cause waiting behaviour with
any combination of other threads' states; this is commonly considered
equivalent to wait-freeness even if it's a weaker qualification.


Interface
---------

`lfht` implements a lock-free hashed intrusive multiset. Callers are required
to be in an open epoch bracket during calls to `lfht_*()` functions, and while
holding any `lfht_iter` expected to remain valid. Closing the bracket
invalidates initialized iterators; attempting to use them leads to undefined
behaviour even if done within a subsequent epoch bracket.

To allow for a sane implementation, `lfht` relaxes some properties of the
basic CCAN `htable`: namely, duplicate entries may appear during iteration.
More formally, an entry that's been added A times and deleted D times may
appear in iteration more than A - D times iff A > D.

Due to the general nature of lock-free data structures, callers will want to
design their own pointer safety mechanisms for pointers they put into the
table. The provided `epoch` module is, in general, equivalent to
read-copy-update; however, other approaches (such as reference counting, and
hazard pointers) should also work.


Performance
-----------

Completely unmeasured.

Generally, applications using lock-free wait-free data structures gain more
from guaranteed progress than they do from scheduler throughput maximization,
or from minimization of allocated memory. Thus, `lfht` is likely to be slower
in both the single-threaded case and compared to a mutexed CCAN `htable` until
there's a significant amount of transitive sleeping on the mutex, which can be
as many as hundreds of threads; all the while heavily pigging out on memory.

On the other hand, `lfht` has much nicer maximum latency characteristics for
write-heavy loads whenever the runtime environment implements `calloc(3)`
lazily, and whenever the system is under such heavy load that preëmption
effects cause knock-on latency in sensitive tasks.

So, go figure.


 -- Kalle A. Sandström <ksandstr@iki.fi>
