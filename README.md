
What
====

This directory contains an implementation of lock-free wait-free
singly-linked lists ("nbsl"), a lock-free hashtable ("lfht"), an epoch
reclamation mechanism, and some tests for each. Out of these, lfht and the
test suites are thought novel as of June 2017, the epoch stuff is probably
described in literature all over, and nbsl's algorithm was described in a 2003
publication (see `nbsl.h` for details).

Licensed under the GNU GPL v3-or-later, found in the COPYING file in this
directory.


!! Warning !! Danger !! Warning !!
----------------------------------

These data structures are neither formally proven to work, exhaustively
tested, nor battle-proven. Lock-free and wait-free algorithms (and their
lock-free/wait-free applications) are fraught with subtle breakage; as such,
users of this code should do their own review and proceed at their own risk.
Caveat lector to the max: this'll bring locusts upon your house.


Design
------

`lfht` is based on a design someone discussed on Reddit a few years ago in the
context of real-time data structures. The basic idea is that when a hash table
becomes full, instead of allocating a new table and migrating all entries over
in a big O(n) loop, allocate a new table and migrate entries one at a time in
the `add` operation. This ensures that the old table will be cleared before
the new one fills up, and makes `add`'s maximum latency mostly consist of
`calloc()` execution.

This table starts out from the basic design of CCAN's htable module by Rusty
Russell, and modifies it to accommodate lookups from multiple tables at the
same time and robust inter-table migration. The twin optimizations of
perfect-bit and common-bits are retained by creating a new table with an
updated mask whenever a conflicting entry would've been added; rehashing when
there are too many deleted items is achieved in the same way.

The design is then extended for lock-free operation by taking five further
bits out of common-bits and using them to communicate inline migration state,
and to separate out a subformat for connecting temporary destination values to
their sources; and by extending `del` and `add` to correctly consume these
intermediate states. The design proper and its transition graph may be
described in the future.

`lfht` has not been rigorously analyzed for wait-freeness, but it's likely
that it won't cause waiting behaviour with any combination of other threads'
states; this is commonly considered equivalent to wait-freeness even if it's
strictly a weaker qualification. That's to say, livelocks are possible.

To my knowledge and limited capacity for reviewing the literature, this design
is novel as of June 2017; but it wouldn't be surprising to find others that
differ only in detail.


Interface
---------

`lfht` implements a lock-free hashed multiset. Its semantics in a
single-threaded program are the same as CCAN's `htable`: storage and lookup of
0..n copies of a given key, where the value is typically accessed by
`container_of()` on the key pointer. In a multithreaded program, `lfht_*()`
calls are specified to complete as long as CPU time and memory are available,
subject to obstructions in the operating system's VM stack but regardless of
the states of other threads in the process.

Callers are required to be in an open epoch bracket during calls to `lfht_*()`
functions, and while holding any `lfht_iter` expected to remain valid. Closing
the bracket invalidates initialized iterators; attempting to use them leads to
undefined behaviour even if done within a subsequent epoch bracket.

Owing to the usual hazards of lock-free data structures, callers will want to
apply a safety mechanism to pointers they put into the table. The provided
`epoch` module is roughly equivalent to read-copy-update without scheduler
cooperation; however, other approaches (such as reference counting, and hazard
pointers) should also work as long as epoch guarantees are maintained also.

To allow for a sane implementation, `lfht` relaxes iteration consistency in
multithreaded programs. Therefore duplicate entries may appear during
iteration, including duplicates of entries that've been recently deleted.
While any thread is running migration, a key added A times and deleted D times
may appear in iteration more than A - D times. However, `lfht_del()` will
succeed exactly A times regardless of the number of applicable threads, and
duplicates vanish as their respective migrations complete. In a typical
program duplicates should be rare, but their numbers increase with the number
of concurrent calls to `lfht_add()`, which may have heretofore undiscovered
coherency bottlenecks.


Performance
-----------

Completely unmeasured.

Generally, applications using lock-free wait-free data structures gain more
from guaranteed progress than they do from scheduler throughput maximization,
or from minimization of allocated memory. This is all the more true when the
algorithm does six writes back and forth to migrate a single item. Thus,
`lfht` is likely to benchmark slower in both the single-threaded case and
compared to a mutexed CCAN `htable` until there's a significant amount of
transitive sleeping on the mutex, which can be as many as hundreds of threads;
all the while heavily pigging out on the VM.

On the other hand, `lfht` has much nicer maximum latency characteristics for
write-heavy loads whenever the runtime environment implements `calloc(3)`
lazily, and whenever the system is under such heavy load that preëmption
effects cause knock-on latency in interactive tasks.

So, go figure.


 -- Kalle A. Sandström <ksandstr@iki.fi>
