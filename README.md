
SolrCloud Manager
=================

Provides protected access to the collections API for cluster management.
 
Command-line syntax, with extra protection against doing Bad Things, for example:
    * deleting the last replica for a given slice
    * creating a collection that doesn't reference a known config in ZK
    * deleting a collection out from under an alias

Also provides some advanced cluster-manipulation actions, see the relevant section below.

Prerequisites:
==============

Assumes you're handling ALL collection configuration data in ZooKeeper.

Assumes Solr >= 4.8. Specifically, the ADDREPLICA collections API command was added in 4.8. Earlier cluster versions
may work if you don't need to add replicas, but this is untested.

Requires Scala & SBT. Installing sbt-extras will handle getting these for you: https://github.com/paulp/sbt-extras

Basic commandline usage:
=======================

Simple commands simply mirror their collections API counterpart, while making sure that the command could (and did) succeed. 
    
    # Show usage: 
    sbt "run --help"

Some examples:

    # Show cluster state:
    sbt "run -z zk0.example.com:2181/myapp"
    # create a collection
    sbt "run createcollection -z zk0.example.com:2181/myapp -c collection1 --slices 2 --config myconf"
    # add a replica
    sbt "run addreplica -z zk0.example.com:2181/myapp -c collection1 --slice shard1 --node server2.example.com:8980_solr"
    # Use any fresh nodes in the cluster to increase your replication factor
    sbt "run fill -z zk0.example.com:2181/myapp -c collection1"
    # Remove any replicas not marked "active"
    sbt "run clean -z zk0.example.com:2181/myapp -c collection1"


Terminology:
============

Terminology here should follow https://wiki.apache.org/solr/SolrTerminology pretty closely, 
but with one significant exception:

There's some confusion between the terms "slice" and "shard" in the Solr code and documentation. This project
prefers "slice" throughout, but does use "shard" in a few places where Solr exposes that word specifically. 

In general, everyone agrees that "slice" should mean the logical partition, and "shard" 
should mean a specific instance of a slice, but solr's naming convention when creating a  
collection names slices "shardX", so this confusion probably isn't going away anytime soon. It's
usually safe to use these words interchangeably to indicate the logical partition.

Additionally, "node" here refers to a physical host machine running solr. A node specifier takes the format
    &lt;IP address&gt;:&lt;port&gt;_solr
Solr always uses the IP address in a node specification, but this tool will do hostname translation if you specify that.
If your servlet isn't bound to /solr, give it a try anyway, but there may be bugs.


Cluster commands:
-----------------

**populate**:
Support for a bulk-deployment strategy for a collection. 

Add a new node to your cluster, create your new 
collection ONLY on that node, and index your data onto it. 
 
This gets you an index without impacting current nodes. 
"populate" at this point will replicate the slices on that single node across the rest of your cluster, and 
optionally clean up by wiping the shards from the index node so you can cleanly remove it from the cluster again.

**fill**:
Adds replicas to nodes not currently participating in a given collection, starting with the slices with the
lowest replica count.
    
The number of replicas per node will not exceed the number of replicas per node for the collection
prior to running this command. May result in uneven replica counts across slices, if extra capacity wasn't 
evenly divisible.
    
**clean**:
Removes any replicas in a given collection that are not currently marked "active". A node doesn't have to be
up for it's replicas to be removed.
    
**copy**:
Provides a method of doing **cross cluster** bulk data deployment. Several caveats:
    
1. The collection must exist in both clusters
1. The collections in each cluster MUST have the same number of slices - hashing MUST be identical.
1. The collection you're copying into should be empty - if it isn't, solr may silently decide a slice is
newer than the data you're trying to copy, and fail to do so.

    
TODO:
=====

* More consistent exception responses and handling (Use ManagerException more)
* Solr version detection
* Better introspection of solr responses
* Insure paths other than /solr work

Issues:
=======

The collections API currently has several issues. 
This tool currently works around some of them, marked (w)

Collections API Issues:

1. SOLR-6072 - DELETEREPLICA can't delete the files from disk (Fixed in Solr 4.10)
1. (w) SOLR-5638 - If you try to CREATECOLLECTION with a configName that doesn't exist, it creates the cores before noticing that
the config doesn't exist. This leaves you with cores that can't initialize, which means:
    1. You can't try again because the desired directories for those cores already exist
    1. Restarting the node may cause the collection to show up with all shards in "Down" state
    1. Trying to delete the collection fails because the cores can't be loaded
    1. You have to stop each node, and remove the core directories
1. (w) SOLR-5128 - Handling slice/replica assignment to nodes must be done manually because the maxShardsPerNode setting can't be 
changed after CREATECOLLECTION.
1. (w) SOLR-5970 - Some collections api requests return status of 0 in the responseHeader, despite a failure. 
(CREATECOLLECTION in non-async, maybe others)
1. (w) If you DELETEREPLICA the **last** replica for a given shard, you get an error like 
   "Invalid shard name : shard2 in collection : collection1", but the replica **is** actually removed from the clusterstate
1. You can have two replicas of the same slice on the same node (This tool provides some additional protection around the ADDREPLICA 
    command, but not at collection creation time)
1. Running a Collection API command using the "async" feature hides the actual error message, if there was one

IntelliJ Issues:

In IntelliJ's SBT import, the artifact inclusion order defined in build.sbt isn't respected properly. 
After importing the project,
Go to File->Project Structure->Modules and make sure the "test-framework" jars appear before the "core" jars.
