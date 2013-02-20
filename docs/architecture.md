Bloomd Ring Architecture
==========================

This document is used to describe the high level architecture of bloomd ring,
and to help provide developers and users of an understanding of how it operates.

To understand the architecture of Bloomd Ring, first some ground terminology and concepts
must be established. Bloomd is a standalone C server that provides a network protocol
for manipulating bloom filters. It supports a total of 11 commands. Within bloomd, there
are individual "filters", which are named by the user. Within a filter, keys can be set and
checked.

The bloomd ring system provides a transparent wrapper layer to bloomd. This means that it is
fully API compatible with bloomd, and clients do not know if they are speaking to a standalone
bloomd or to a bloomd ring. Behind the scenes, bloomd ring uses a Dynamo distribution technique
first described by Amazon. In this case, bloomd ring is built on Riak Core which is modeled closely
on Dynamo.

Being based on Dynamo, we write each key to N nodes (in this case, N=3). For writes to succeed,
at least W nodes (W=2) have to accept the right. For reads, we ask all N ndoes, but as long as
R nodes (R=2) respond, we can service the request. Because of this architecture, bloomd ring is
highly available, as node failures can be easily tolerated. In addition, bloomd ring automatically
partitions the key space into many sub-filters. For example, if the client is writing to the "foo",
filter, bloomd ring will split the filter into K slices (K=ring size). So if the ring size is 256,
then there will exist "foo:00" up to "foo:ff". This means both that each individual filter is 256 times
small than an equivilent single filter, but that the slices can be spread across the entire cluster.
This allows for much higher scalability of any given failure, but also reduces the amount of data
loss due to losing any given slice.


Command Handling Details
=========================

In this section we try to detail the behavior of bloomd ring for each given command
that is supported by bloomd. This is to provide greater insight into the cost and
availability of each command.

create
------

The create command is used to create a new filter. Because of the slicing behavior of
bloomd ring, this will create K new filters, each of which exist on a quorum of nodes.
So, in total, up to K*N `create` commands must be issued to the underlying bloomd instances.

In practice this means that this is a rather expensive operation as it involves all nodes
in the cluster. Luckily, it is done relatively infrequently.

Because bloomd ring must be able to migrate slices between nodes, doing in-memory filters
is not possible. For this reason, attempting to create an in-memory only filter will fail.

Additionally, if the filter is specified with an initial capacity, that capacity will be
divided by K and then issued. So, if the client tries to create a filter with capacity of
256MM, and K=256, we instead create 256 filters of size 1MM.

list
-----

The list operator is used to find all the available filters in the system, as well
as some useful information about size, usage, etc. This command requires _coverage_.
This means, that 1/N nodes need to be queried to determine all the available filters.

There is also an extension implementation that provides more accurate information.
This mode forces a full-cluster query instead of just coverage. It is invoked using

    list +absolute


drop
-----

Drop command is used to delete a filter. Like a create command, it actually needs to
delete K different filters, so a total of N*K deletes take place across the entire
cluster.

close
------

Close is used to map a filter out of memory and keep it on disk. Like drop, this
must talk to the entire cluster.

clear
------

Clear is used to remove a filter from bloomd's management, but lets it live on disk.
Meaning it is a non-destructive drop. It operates similarly and involves the entire
cluster.

check | c
----------

The check command is used to see if a given key exists in a filter. This command
creates a quorum and only needs to communicate with N different nodes.

multi | m
----------

The multi command is used to see if a set of given keys exists in a filter.
This gets decomposed into multiple checks that run in parallel. Each check
needs to talk to N nodes. In the future, bloomd ring can try to detect which
keys exist on the same slice and attempt to intelligently use multi commands to
reduce traffic, but it is an internal implementation detail that should not be
relied upon.

set | s
--------

Set command operates the same as the check command, but it sets a key in a filter.
It must also communicate to N nodes.

bulk | b
---------

The bulk command is similar to multi, and it also decomposes to multiple set commands.
It could also intelligently use bulk commands, but that is an internal detail. It will
do 1 set per key, and each set requires N nodes to be involved.

info
-----

The info command gets statistical information about a filter. This command implements
a minor extension to bloomd, which only makes sense in the context of bloomd ring.

In the standard mode, it requires communicating with a covering set, or 1/N nodes.
In the extended mode, it will communicate to all nodes and provide absolutely correct
information.

In standard mode, we communicate with 1/N of the nodes, and then based on this we
extrapolate the statistical information approximately. For example, if 1/3 of the nodes
report a total of 3MM checks, bloomd ring will estimate that 9MM checks have occured.

In absolute mode, every slice is queried, and the statistical information is the sum
of all the slices. Absolute mode is specified by issuing the command as:

    info filter +absolute

flush
------

Flush is used to force a specific filter or all filters to persist to disk. If a flush
is issued without specifying a filter, then we issue a flush to all nodes, and this involves
the entire cluster. If a specific filter is named, then each slice of that filter must be
flushed, which also involves the entire cluster, but NOT other slices that are co-located.


Bloomd Ring Command Processing Pipeline
========================================

Each layer of bloomd ring is well defined to provide a seperation of concerns.

The first layer of the stack, is the bloomd command parser. This is composed of a process to
accept connections, and a process-per-client model. Each new client gets a process which parses
incoming commands. Once a command has been processed, we start an appropriate co-ordinator to handle
the command. The parser handles each command in a strictly serial fashion, so as to prevent
unexpected behavior.

There are a number of different co-ordinators based on the command. Some commands involve the
entire cluster, a covering set, or just a quorum. The co-ordinators then manage the interaction
between the parser and the v-nodes on the target machines.

Each of the v-nodes understand how to process the 11 commands bloomd speaks. They make use of the
erl-bloomd library to speak to a bloomd instance running on the same box.

        ----------------------
        |     Client         |
        ----------------------
                   | TCP
                   v
        ----------------------
        |   Bloomd Parser    |
        ----------------------
                   |
                   v
        ----------------------
        |   Coordinator      |
        ----------------------
            |      |     |  Over Network
            v      v     v
        ----------------------
        |     V Node (s)     |
        ----------------------
                   |  erl-bloomd / local tcp
                   v
        ----------------------
        |      Bloomd        |
        ----------------------


Failure Handling Model
=======================

Becuase of the Dynamo model, all writes make it to at least N v-nodes. In case of a node
failure, bloomd ring makes no attempt to use fall back nodes. Instead, the remaining primaries
are resposible until the node is recovered. The reason we avoid using fallbacks has to due with
the nature of bloom filters. Bloom filters are one-directional, meaning given keys you can generate
the bloom filter, but given the filter you cannot reverse it to get the keys. Therefor, if we had
failovers during a node failure, merging the data on the failover with the primary becomes extremely
challenging since we cannot reconstruct the original key set.

As a future optimization, failover nodes could write all the keys to a log file,
and on recovery, replay all the keys on the primary. This behavior is not expected in the initial
version, and instead failover will not take place.


Handoff for Bloomd
===================

Implementing hand off is a critical aspect of allowing bloomd ring to gracefully
grow and shrink the cluster. First we define the source node S and the new destination
node as D. When handoff from S to D is initiated, on node S we stop writing to bloomd,
and instead forward all writes to D. At the same time, we issue a `flush` on the local
filters, and then begin to copy the data files to D. Once all the data files are copied over,
we fault them in on D by issuing a `create` to the bloomd instance on D. Once the filter
is faulted in on D, we then replay all the writes that D received to apply them to the filter.
Once this is completed, we issue a `drop` on S to delete all the local files and reclaim the
memory used.

This allows S to continue to service reads, and D is able to take over the vnode without
any writes being dropped.


Key Hashing
============

A very important aspect of Dynamo systems is hashing the data in a manner
that leads to a good distribution of data across the system. In the case of
bloomd ring, most commands are oriented around filters and keys.

Each filter is decompossed into K sub-filters or slices, which represent
parts of the key space.

To determine which v-node owns a particular filter/key, we do the following:

    Slice = Hash(Key) % K
    V-Node = Hash({Filter, Slice})


