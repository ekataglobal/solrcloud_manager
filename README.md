
SolrCloud Manager
=================

Provides protected access to the collections API for cluster management. 
Command-line syntax, with extra protection against doing Bad Things like deleting the last replica for a
given slice, or creating a collection that doesn't reference a known config in ZK, or deleting a collection
out from under an alias.
Also provides some advanced cluster-manipulation actions.

Assumes you're handling all collection configuration in ZooKeeper.
Assumes Solr >= 4.8 (Specifically, it uses the ADDREPLICA collections API.)

Basic commandline usage:
=======================


Most commands simply mirror their collections API counterpart, making sure that the command could (and did) succeed. 
    
    # Show usage: 
    sbt run --help

Assuming one of your solrcloud ZK instances is hosted in the "myapp" chroot on zookeeper0.example.com

    # Show cluster state:
    sbt run -z zookeeper0.example.com:2181/myapp

Cluster commands:
-----------------

**populate**:
    Support for a bulk-deployment strategy for a collection. Add a new node to your cluster, create your new 
     collection ONLY on that node, and index your data onto it. This gets you an index without impacting current nodes. 
     "populate" at this point will replicate the slices on that single node across the rest of your cluster, and 
     optionally clean up by wiping the shards from the index node so you can cleanly remove it from the cluster again.

**fill**:
    Adds replicas to nodes not currently participating in a given collection, starting with the slices with the
    lowest replica count.
    The number of replicas per node will not exceed the number of replicas per node for the collection
    prior to running this command. May result in uneven replica counts, if extra capacity wasn't evenly divisible.
    
**clean**:
    Removes any replicas in a given collection that are not currently marked "active". A node doesn't have to be
    up for it's replicas to be removed.

    
TODO:
=====

* Proper output handling (logging)
* More consistent exception responses and handling (Use ManagerException more)
* Solr version detection
* Better introspection of solr responses

Issues:
=======

The collections API currently has several issues. 
This tool currently works around most of them, marked (w)

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
1. (w) SOLR-5970 - Some collections api requests return status of 0 in the responseHeader, despite a failure. (CREATECOLLECTION in non-async, maybe others)
1. (w) If you DELETEREPLICA the **last** replica for a given shard, you get an error like 
   "Invalid shard name : shard2 in collection : collection1", but the replica **is** actually removed from the clusterstate
1. You can have two replicas of the same slice on the same node (This tool provides some additional protection around the ADDREPLICA 
    command, but not at collection creation time)

IntelliJ Issues:

In IntelliJ's SBT import, the artifact inclusion order defined in build.sbt isn't respected properly. 
After importing the project,
Go to File->Project Structure->Modules and make sure the "test-framework" jars appear before the "core" jars.