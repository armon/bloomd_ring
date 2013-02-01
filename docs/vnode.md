Bloomd V-node Architecture
==========================

This document is used to document the architecture and capabilities
of a v-node in the bloomd-ring. It is more oriented for developers
than for users to provide an understanding of how it operates.


Command Handling Details
=========================

In this section we try to detail the behavior of the vnode for each given command.

create
------

A vnode is signaled to create when it receives a message like:

    {create_filter, FilterName, Slices, Options}

The vnode should then create each of the slices for the given filter,
with any possible options specified. The response should be one of:
* done : If any slices needed to be created
* exists : Only if all slices already exist
* {error, command_failed}


list
----

A vnode is signaled to do a list of filters when it receives a message
like:

    list_filters

However, due to the slicing that takes place, there maybe a lot of
duplicated data. First, it is necessary to determine the set of
unique filters. Next, the per-slice summary information should be
provided to the co-ordinator (probability, storage, capacity, size).

The result format is:

    {ok, [
    {FilterName, [{Slice, Probability, Storage, Capacity, Size}]}
    ]}

Alternatively, {error, command_failed}.

The response should be one of:
* {ok, [{FilterName, [{Slice, Probability, Storage, Capacity, Size}]}]}
* {error, command_failed}

drop
-----

A vnode is signaled to do a drop when it receives a message like:

    {drop_filter, FilterName}

The vnode may contain multiple slices per filter, and to support this,
the vnode may need to do a list, find all the relevant slices, and
drop them all. `done` should only be returned if all slices were
deleted, or do not exist. Otherwise an appropriate error should be
returned.

The result is one of:
* done : If any slice is deleted
* {error, no_filter} : Only if there is no slice
* {error, command_failed}

close
-----

A vnode is signaled to do a close when it receives a message like:

    {close_filter, FilterName}

The vnode may contain multiple slices per filter, and to support this,
the vnode may need to do a list, find all the relevant slices, and
close them all. `done` should only be returned if all slices were
closed. Otherwise an appropriate error should be returned.

The result is one of:
* done : If all slices are closed
* {error, no_filter} : Only if there is no slice
* {error, command_failed}

clear
-----

A vnode is signaled to do a clear when it receives a message like:

    {clear_filter, FilterName}

The vnode may contain multiple slices per filter, and to support this,
the vnode may need to do a list, find all the relevant slices, and
clear them all. `done` should only be returned if all slices were
cleared. Otherwise an appropriate error should be returned.

The result is one of:
* done : If all slices are cleared
* {error, no_filter} : Only if there is no slice
* {error, not_proxied} : If any slice is not proxied. The vnode should
  make a best effort to 'rollback', by issuing a create on any slices
  that were closed.
* {error, command_failed}

check
-----

A vnode is signaled to do a check when it receives a message like:

    {check_filter, FilterName, Slice, Key}

In this case, the slice is known ahead of time. The vnode can simply
query the local bloomd instance directly and return the result.

The result is one of:
* {ok, bool()}
* {error, no_filter}
* {error, command_failed}

set
---

A vnode is signaled to do a set when it receives a message like:

    {set_filter, FilterName, Slice, Key}

In this case, the slice is known ahead of time. The vnode can simply
query the local bloomd instance directly and return the result.

The result is one of:
* {ok, bool()}
* {error, no_filter}
* {error, command_failed}

info
----

A vnode is signaled to get info when it receives a message like:

    {info_filter, FilterName}

The vnode may contain multiple slices per filter, and to support this,
the vnode may need to do a list, find all the relevant slices, and
query them all.

An info command typically returns the following:
* capacity
* checks
* check_hits
* check_misses
* in_memory
* page_ins
* page_outs
* probability
* sets
* set_hits
* set_misses
* size
* storage

The return value should be a list containing the results for each slice:

    {ok, [{Slice, Properties}]}

This way, the co-ordinator is able to properly resolve the information.
Because it is possible for multiple v-nodes to be responsible for a
slice that is using a shared bloomd instance, we let the co-ordinator
handle the de-duplication.

The result should be one of:
* {ok, [{Slice, Properties}]}
* {error, no_filter} : If there are no slices
* {error, command_failed}

flush
-----

A vnode is signaled to do a flush when it receives a message like:

    {flush_filter, FilterName}

The vnode may contain multiple slices per filter, and to support this,
the vnode may need to do a list, find all the relevant slices, and
flush them all. `done` should only be returned if all slices were
flushed. Otherwise an appropriate error should be returned.

It is possible for FilterName to be specified as `all`. In this case,
the vnode should issue a flush with no filter to the local bloomd
instnace, and thus cause all the filters to be flushed.

The result is one of:
* done : If all slices are flushed
* {error, no_filter} : Only if there is no slice
* {error, command_failed}

multi and bulk
-----

In the current implementation, the co-ordinators and clients
break a single multi and bulk into many sub-checks and sub-sets.
This means that the vnodes never directly need to implement
support for multi or bulk.


Handoff Consideration
======================

In the case of a handoff taking place, there are several additional
considerations that a vnode must make.

Handoff start
-------------

When a hand-off start takes place, there are several things that must
take place:
* Writes to the affected slices must stop
* Affected slices should be flushed
* Writes to slices must be forwarded to new node
* Reads from slice may be serviced normally

What this means:
* drop / close / clear is disallowed on the slice
* {set_filter, } has different logic
* flush could make v-node unresponsive for a while


Handoff data transfer
---------------------

To transfer the existing data, we must reach into the bloomd
data directory, and copy the existing files for the slice.
The files are copied into the same location on the new node.

What this means:
* We expect the data files to not be modified
* Huge files, expect high network traffic


Handoff completion
-------------------

Once data handoff is completed, the source v-node can issue
a 'drop' on the slice, to both recycle the memory and delete
the data from disk.

The destination v-node must issue a 'create' which will reload
the slice. Once the slice is in memory, the writes which were
previously forwarded must be applied.

What this means:
* The first 'create' may be very slow
* Applying the old sets may be time intensive


Improvements
-------------------------

There are several things that could be done to improve
the overall responsiveness of the system. These are not
necessarily part of the original design goals.

* flush should not block the bloomd event loop
* create should not block the bloomd event loop


Determining Slice Ownership
============================

Because bloomd ring relies on a single instance of bloomd
running on the local node, there is an issue of multi-tenancy,
and thus it introduces the problem of ownership.

If the local bloomd instance has 50 filters, and there are
30 v-nodes running locally, it can be important to determine
which slices belong to which v-nodes for certain operations.

To do this, a v-node performs a `list` to get all the filters.
Then, by applying the distribution hash function to each filter,
the owning v-node can be determined. A particular v-node can
compare this to it's own ID to check if it owns a given slice.


