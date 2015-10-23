
SolrCloud Manager
=================

Provides easy SolrCloud cluster management.
 
Command-line syntax, layers two major features on the Solr Collections API:

1. An abundance of caution. Some example Bad Things this helps prevent:
    * Deleting the last replica for a given slice
    * Creating a collection that doesn't reference a known config in ZK
    * Deleting a collection with a current alias
2. Ease of use:
    * Support for common cluster-manipulation actions, see the "Cluster Commands" section below.
    * Display and refer to nodes by DNS names, instead of IP addresses. 

Prerequisites:
==============

Assumes you're handling ALL collection configuration data in ZooKeeper.

Solr >= 4.8. Specifically, the ADDREPLICA collections API command was added in 4.8. Earlier cluster versions
may work if you don't need to add replicas, but this is untested.

If your servlet isn't bound to /solr, give it a try anyway, but there may be bugs.

Installing:
===========

Download the assembly jar from one of the github releases, and run using java, like:

    java -jar solrcloud_manager-assembly-1.2.0.jar --help
    

Basic usage:
=======================

Simple commands simply mirror their collections API counterpart, while making sure that the command could (and did) succeed. 
    
    # Show usage: 
    java -jar solrcloud_manager-assembly-1.2.0.jar --help

Some examples:

    # Show cluster state:
    java -jar solrcloud_manager-assembly-1.2.0.jar -z zk0.example.com:2181/myapp
    
    # create a collection
    java -jar solrcloud_manager-assembly-1.2.0.jar -z zk0.example.com:2181/myapp -c collection1 --slices 2 --config myconf

    # add a replica
    java -jar solrcloud_manager-assembly-1.2.0.jar addreplica -z zk0.example.com:2181/myapp -c collection1 --slice shard1 --node server2.example.com
    
    # Use any unused/underused nodes in the cluster to increase your replication factor
    java -jar solrcloud_manager-assembly-1.2.0.jar fill -z zk0.example.com:2181/myapp -c collection1
    
    # Remove any replicas not marked "active"
    java -jar solrcloud_manager-assembly-1.2.0.jar cleancollection -z zk0.example.com:2181/myapp -c collection1
    
    # Move all replicas for all collections from one node to another 
    java -jar solrcloud_manager-assembly-1.2.0.jar migrate -z zk0.example.com:2181/myapp --from node1.example.com --onto node2.example.com


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

    <IP address>:<port>_solr

Solr always uses the IP address in a node specification, but this tool will do hostname translation
if possible, and will select nodes from incomplete specifications if a distinct identifier can be found.


Cluster commands:
-----------------

A lot of the work in large Solrcloud cluster management isn't so much figuring out the right API command, 
as figuring out the correct **set** of API commands. See the help output for a complete list, but the following 
common cluster operations are supported:

**fill**:
Adds replicas to nodes not currently/fully participating in a given collection, starting with the slices with the
lowest replica count.
    
The number of replicas per node will not exceed the number of replicas per node for the collection
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
    
**waitactive**:
This command returns when all replicas for the given node are active. No cluster changes are made. 
Useful as a mechanism for delaying other actions. For example, preventing a rolling restart from overstepping
your replication factor.

**backupindex**:
For a given collection, save a copy of your index data onto the local disk of the nodes. This can protect you
from user error like accidental deletion, but if you want to protect against hardware failure you'll still need
a way to ship the backup copy off of the nodes somehow. Giving all your nodes a shared remote 
filesystem (ie, NFS) and specifying that as the backup directory would work.

**restoreindex**:
For a given collection, restore a backup created by this tool into that collection. Assumes the backup
was created by this tool, and that the directory was a shared filesystem.
    
**copy**: (Experimental)
Provides a method of doing **cross cluster** bulk data deployment. Several caveats:
    
1. The collection MUST exist in both clusters.
1. The collections in each cluster MUST have the same number of slices. Hashing MUST be identical, and there's 
no check for this!
1. The collection you're copying into should be empty - if it isn't, solr may silently decide a slice is
newer than the data you're trying to copy, and fail to do so.
1. Don't forget to hard-commit on the cluster you're copying from before you start

**populate**: (Experimental)
Support for a bulk-deployment strategy for a collection. 

Add a new node to your cluster, create your new 
collection ONLY on that node, and index your data onto it. 
 
This gets you an index without impacting current nodes. 
"populate" at this point will replicate the slices on that single node across the rest of your cluster, and 
optionally clean up by wiping the shards from the index node so you can cleanly remove it from the cluster again.

    
TODO:
=====

* Pretty-print operation output
* More consistent exception responses and handling (Use ManagerException more)
* Cross version support (where possible)
* Better introspection of solr responses
* Insure paths other than /solr work

SolrCloud Issues:
=======

As of Solr 4.9, The collections API currently has several issues. 
This tool can work around some of them, marked (w).

1. SOLR-6072 - DELETEREPLICA can't delete the files from disk (Fixed in Solr 4.10)
1. (w) SOLR-5638 - If you try to CREATECOLLECTION with a configName that doesn't exist, it creates the cores before noticing that
the config doesn't exist. This leaves you with cores that can't initialize, which means:
    1. You can't try again because the desired directories for those cores already exist
    1. Restarting the node may cause the collection to show up with all shards in "Down" state
    1. Trying to delete the collection fails because the cores can't be loaded
    1. You have to stop each node, and remove the core directories
1. (w) SOLR-5128 - Handling slice/replica assignment to nodes must be done manually because the maxShardsPerNode setting can't be 
changed after CREATECOLLECTION. As a result, this tool ignores maxShardsPerNode completely, aside from CREATECOLLECTION.
1. (w) SOLR-5970 - Some collections api requests return status of 0 in the responseHeader, despite a failure. 
(CREATECOLLECTION in non-async, maybe others)
1. (w) If you DELETEREPLICA the **last** replica for a given shard, you get an error like 
   "Invalid shard name : shard2 in collection : collection1", but the replica **is** actually removed from the clusterstate,
   leaving you with a broken collection
1. You can have two replicas of the same slice on the same node (This tool provides some additional protection around the ADDREPLICA 
    command, but not at collection creation time)
1. Running a Collection API command using the "async" feature hides the actual error message, if there was one

Development:
============

Requires Scala & SBT. Installing sbt-extras will handle getting and managing these for you: https://github.com/paulp/sbt-extras

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
