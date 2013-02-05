Bloomd Co-ordinators Architecture
===================================

This document is used to provide an overview of the architecture
and capabilities of the various co-ordinators present in the bloomd ring.
It is more oriented for developers than users for providing an understanding
of how it operates.

Firstly, there are 3 different co-ordinator behaviors that are used to
services the various commands:
* Full Cluster coordinator
* Covering set coordinator
* Slice coordinator

Each co-ordinator is covered in more detail below.

Slice Co-ordinator
===================

The slice co-ordinator is a FSM that is used to co-ordinate commands
that only affect a single slice. The primary use for this is check and set
commands, and by extension, the multi and bulk commands. Since these commands
operate on individual keys, we can determine the relevant slices and only
talk to those.

This co-ordinator thus determines the N partitions that are releveant,
and waits to hear back from at least R/W before responding. In the case
of bloomd ring, there is no flexibility to specify these in the command,
and it is currently up to the server-side to determine appropriate values.

Full Cluster Co-ordinator
==========================

The full cluster co-ordinator is a FSM that is used for commands that
require the entire cluster. For the most part, this includes any CRUD
operation on a filter. (create, drop, close, clear, flush, info*).

The behavior of this co-ordinator is to send the command to all nodes
and wait until all the nodes respond. There is never a case that requires
a read repair, so the FSM implementation is fairly simple.

*info does not always require a full cluster co-ordinator, only in the case
that the +absolute extension is used for more accurate values.

Covering set coordinator
========================

The covering set co-ordinator is an FSM that is used to co-ordinate
commands that generally may not be fully accurate and only need to
be approximately correct. This includes the list and info* command.

The behavior of this co-ordinator is to send the command to a covering
set, and attempt to fall back onto other nodes until the request can
be serviced. Because of the nature of commands that use this, read
repair is never performed.

*info does not always use a covering set, in the case of the +absolute
extension, it will use a full-cluster co-ordinator instead.

