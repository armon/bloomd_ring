bloomd\_ring: A Riak Core Application for Bloomd [![Build Status](https://travis-ci.org/armon/bloomd_ring.png)](https://travis-ci.org/armon/bloomd_ring)
======================================

bloomd_riak is a Riak core application that provides an interface which is compatible
with [bloomd](https://github.com/armon/bloomd), but transparently shards data across a cluster
of nodes. Using the Riak Core / Dynamo architecture, it is possible to use bloom filters in a highly
scalable and fault tolerant manner. The individual vnodes in the cluster proxy the requests to a
backend instance of bloomd running on each node. The reason for this is that the bloomd code base
has proven to be highly reliable and extremely performant.

Application Structure
---------------------

This is a blank riak core application. To get started, you'll want to edit the
following files:

* `src/riak_bloomd_vnode.erl`
  * Implementation of the riak_core_vnode behaviour
* `src/bloomd.erl`
  * Public API for interacting with your vnode
