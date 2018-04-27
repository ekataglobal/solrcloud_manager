
SolrCloud Manager
=================

Provides easy SolrCloud cluster management.
 
Command-line syntax, layers two major features on the Solr Collections API:

1. An abundance of caution. Some example Bad Things this helps prevent:
    * Deleting the last replica for a given slice ([SOLR-8257](https://issues.apache.org/jira/browse/SOLR-8257))
    * Creating a collection that doesn't reference a known config in ZK ([SOLR-5638](https://issues.apache.org/jira/browse/SOLR-5638))
    * Deleting a collection with a current alias
2. Ease of use:
    * Support for common cluster-manipulation actions, see the "Cluster Commands" section below.
    * Display and refer to nodes by DNS names, instead of IP addresses. 

Prerequisites:
==============

Assumes you're handling ALL collection configuration data in ZooKeeper.

Solr >= 4.8, including 5.x. Specifically, the ADDREPLICA collections API command was added in 4.8. Use with Solr 6.x is currently untested.

Earlier cluster versions may work out of the box if you don't need to add replicas, but this is untested. 
Full support for earlier 4.x versions could probably be achieved by falling back to the core 
admin API, but this is unwritten.  

Installing:
===========

Download the assembly jar from one of the github releases, and run using java, like:

    java -jar solrcloud_manager-assembly-1.7.0.jar --help
    

Basic usage:
=======================

Simple commands simply mirror their collections API counterpart, while making sure that the command could (and did) succeed. 
    
    # Show usage: 
    java -jar solrcloud_manager-assembly-1.7.0.jar --help

Some examples:

    # Show cluster state:
    java -jar solrcloud_manager-assembly-1.7.0.jar -z zk0.example.com:2181/myapp
    
    # create a collection
    java -jar solrcloud_manager-assembly-1.7.0.jar -z zk0.example.com:2181/myapp -c collection1 --slices 2 --config myconf

    # add a replica
    java -jar solrcloud_manager-assembly-1.7.0.jar addreplica -z zk0.example.com:2181/myapp -c collection1 --slice shard1 --node server2.example.com
    
    # Use any unused/underused nodes in the cluster to increase your replication factor
    java -jar solrcloud_manager-assembly-1.7.0.jar fill -z zk0.example.com:2181/myapp -c collection1
    
    # Remove any replicas not marked "active"
    java -jar solrcloud_manager-assembly-1.7.0.jar cleancollection -z zk0.example.com:2181/myapp -c collection1
    
    # Move all replicas for all collections from one node to another 
    java -jar solrcloud_manager-assembly-1.7.0.jar migrate -z zk0.example.com:2181/myapp --from node1.example.com --onto node2.example.com


Command list
=============

The --help output is the definitive list, but here's an overview:

Collections API
---------------

Some of the standard collections API commands can be used via this command line. 

* createcollection (CREATE)
* deletecollection (DELETE)
* alias (CREATEALIAS)
* deletealias (DELETEALIAS)
* addreplica (ADDREPLICA)
* deletereplica (DELETEREPLICA)

Most of the typical options are supported, and in a few cases the extra checks in this tool can help 
you avoid bugs in the collections API.

View commands
-------------

These don't change cluster state.

**clusterstatus**
Prints the current state of the cluster, including:

* Cluster version
* Current overseer
* Current aliases
* Config sets available, and used
* All collections and replicas, with the current state of each 

**clusterhealth**
Runs a series of checks intended to spot problems in your current cluster state.

**waitactive**:
This command returns when all replicas for the given node are active. Useful as a mechanism for delaying 
other actions. For example, preventing a rolling restart from overstepping your replication factor.

Cluster commands
-----------------

A lot of the work in Solrcloud cluster management isn't so much figuring out the right Collections API command, 
it's figuring out the correct **set** of API commands. 

**fill**:
Adds replicas to nodes not currently/fully participating in a given collection, starting with the slices with the
lowest replica count.
    
By default, the number of replicas per node will not exceed the number of replicas per node for the collection
prior to running this command. May result in uneven replica counts across slices, if extra capacity wasn't 
evenly divisible.

Note that "participation" for a node is determined by slice count for the _given_ collection, not slice 
count across _all_ collections. 

**migratenode**
Moves all replicas from one node to another. (So, Copy/Delete)

**clean**
Removes all replicas from a given node.
    
**cleancollection**:
Removes any replicas in a given collection that are not currently marked "active". A node doesn't have to be
up for its replicas to be removed. 
WARNING: Replicas in the "recovering" state are not currently considered active.
    
**copycollection**:
Provides a method for copying collection index data into another collection, even **across solrcloud clusters**! 
Some caveats: 
    
1. Both collections MUST already exist.
1. Both collections should use the same config data. Some differences can be tolerated, using similar heuristics
 to those if you were modifying an existing collection.
1. The collections MUST have the same number of slices, and hashing MUST be identical. 
The replication factor can be different.
1. The collection you're copying into should be empty - if it isn't, solr may silently decide a slice is
newer than the data you're trying to copy, and fail to do so.
1. If copying across clusters, the cluster you're copying INTO must be able to make requests to the cluster you're 
copying FROM.

**clonecollection**
Creates a new (empty) collection using another collection as a template for the various settings. 
The cloned collection may be in another solrcloud cluster completely. If the named configset
for the template collection does not also exist in the target cluster, it will be copied over first.
The resulting collection from this command is a suitable target for a copycollection command.

Backup commands
----------------

**backupindex**:
For a given collection, save a copy of your index data onto the local disk of the nodes. This can protect you
from user error like accidental deletion, but if you want to protect against hardware failure you'll still need
a way to ship the backup copy off of the nodes somehow. Giving all your nodes a shared remote 
filesystem (ie, NFS) and specifying that as the backup directory would work.

**restoreindex**:
For a given collection, restore a backup created by this tool into that collection. Assumes the backup
was created by this tool, and that the directory was a shared filesystem like an NFS mount.
WARNING: A given backup may only be restorable ONCE without some other action on your part. See SOLR-8449.
    

Config commands
---------------

**upconfig** 
Uploads a given local configset directory to ZK.

**downconfig**
Copies a given configset in ZK to a local directory.

**rmconfig**
Deletes a given configset from ZK. (But only if it not referenced by any collection.)

    
TODO:
=====

* More consistent exception responses and handling (Use ManagerException more, better error messaging)
* Better introspection of solr responses
* Insure paths other than /solr work
* Lighter weight test suite

SolrCloud Issues:
=======

Here's a list of some of the Solr Collections API issues I'm aware of. 
This tool can work around or protect against some of them, marked (w).

1. SOLR-6072 - DELETEREPLICA doesn't delete the files from disk (Fixed in Solr 4.10)
1. (w) SOLR-5638 (Fixed in at least 5.4) - If you try to CREATECOLLECTION with a configName that doesn't exist, it creates the cores before noticing that
the config doesn't exist. This leaves you with cores that can't initialize, which means:
    1. You can't try again because the desired directories for those cores already exist
    1. Restarting the node may cause the collection to show up with all shards in "Down" state
    1. Trying to delete the collection fails because the cores can't be loaded
    1. You have to stop each node, and remove the core directories
1. (w) SOLR-5128 - Handling slice/replica assignment to nodes must be done manually because the maxShardsPerNode setting can't be 
changed after CREATECOLLECTION. As a result, this tool ignores maxShardsPerNode completely, aside from CREATECOLLECTION.
1. (w) SOLR-5970 - Some collections api requests return status of 0 in the responseHeader, despite a failure. 
(CREATECOLLECTION in non-async, maybe others)
1. (w) SOLR-8257 - If you DELETEREPLICA the **last** replica for a given slice, you get an error like 
   "Invalid shard name : shard2 in collection : collection1", but the replica **is** actually removed from the clusterstate,
   leaving you with a broken collection
1. You can have two replicas of the same slice on the same node (This tool provides some additional protection around the ADDREPLICA 
    command, but not at collection creation time)
1. Running a Collection API command using the "async" feature can hide the actual error message, if there was one

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

    <IP address>:<port>_<hostcontext>
    ie:
    127.0.0.1:8983_solr

By default, Solr uses the IP address in a node specification. This tool will do hostname translation
on command inputs if possible, and will identify nodes from an incomplete specification if a distinct 
identifier can be found:

    # works fine, unless you have solr running on multiple ports on that machine
    host1.example.com
    # This is fine too, if you only have one server with "host1" in the name
    host1
    # Or you can specify "any node with no replicas of any collection" using the literal string 'empty'
    empty
    # Or you can specify "any node" using the literal string 'all'
    all
    # Or you can specify a regex
    regex=host[1-5].*


Development:
============

Requires Scala & SBT. Installing sbt-extras will handle getting and managing these for you: https://github.com/paulp/sbt-extras

Tool integration
-----------------

Although the primary interface is the command line, the command line is mostly just there to provide setup for 
library code. This means all commands can be integrated into other java/scala applications as well.
Build the jar, and include it in your project. Then write some code like:

    // Scala...
    
    // Create a cluster manager
    val clusterManager = ClusterManager(zkStr)
    
    // set up some Actions you want to do
    val addReplicaAction = AddReplica(collection, slice, node)
    val deleteReplicaAction = DeleteReplica(collection, slice, node2)
    // wrap them in an Operation
    val operation = Operation(List(addReplicaAction, deleteReplicaAction))
    // execute that Operation against your cluster
    operation.execute(clusterManager)
    
    // or use one of the Operations static methods to build you an Operation.
    val fillOp = Operations.fillCluster(clusterManager, collection)
    fillOp.execute(clusterManager)
    
    // Java...  
    // (Note: There hasn't been any focus on java interop. If you want/use this, please say so)
    
    // Create a cluster manager
    ClusterManager clusterManager = new ClusterManager(zkStr);
    
    // set up some Actions you want to do
    AddReplica addReplicaAction = new AddReplica(collection, slice, node);
    DeleteReplica deleteReplicaAction = new DeleteReplica(collection, slice, node2);
    // wrap them in an Operation
    ArrayList lst = new ArrayList<Action>(2);
    lst.add(addReplicaAction);
    lst.add(deleteReplicaAction);
    Operation operation = new Operation(lst);
    // execute that Operation against your cluster
    operation.execute(clusterManager);
    
    // or use one of the Operations static methods to build you an Operation.
    Operation fillOp = Operations.fillCluster(clusterManager, collection);
    fillOp.execute(clusterManager);
    

IntelliJ Issues
---------------

In IntelliJ's SBT import, the artifact inclusion order defined in build.sbt isn't respected properly. 
After importing the project,
Go to File->Project Structure->Modules and make sure the "test-framework" jars appear before the "core" jars.

Running in SBT
--------------

If running in sbt, you may get an exception message "sbt.TrapExitSecurityException thrown from the UncaughtExceptionHandler in thread"
at the end of execution. This is due to the code using System.exit in the SBT context, and can be ignored.

Testing
-------

This runs the test suite:

    sbt test
    
The test suite is built on top of the JUnit framework provided by Solr itself. 
Unfortunately, this is very spammy output, and the bridge between JUnit and sbt causes some strangeness, 
including odd test count output.

Other Tools
===========

It seems like everyone with a significant SolrCloud deployment has written some variant on management tools, but few
of them have been open sourced. Here's what I'm aware of:

*  [solrcloud-haft](https://github.com/bloomreach/solrcloud-haft) Primarily intended as a java library for use in other programs. Currently tested with Solr 4.6.1.
