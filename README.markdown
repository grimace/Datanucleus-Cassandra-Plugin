Cassandra Datanucleus Plugin
============================

Features
--------

* Basic CRUD
* Graph persistence of all loaded objects
* Cascading deletes of all dependent objects
* Subclass retrieval and persistence
* Basic secondary indexing with simple terms.  && || < <= > >= and == are supported.
* In memory ordering and paging.  Paging is not supporting without an ordering clause


See all unit/integration tests for details on how to annotate objects, as well as correctly query them.

Defining repositories
---------------------

Add the following repository to your pom.xml

  <repository>
        <id>maven.spidertracks.com</id>
        <name>Scale7 Maven Repo</name>
        <url>http://github.com/tnine/m2repo/raw/master</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
  </repository>
    
    
Add this dependency for the latest release build

	<dependency>
		<groupId>com.spidertracks.datanucleus</groupId>
		<artifactId>cassandra</artifactId>
		<version>0.7.0-rc1</version>
	</dependency>





Usage
=====

Developing Unit/Integeration Tests
----------------------------------

Include the "test-jar" in your test depdency scope.  Extend or duplicate com.spidertracks.datanucleus.CassandraTest to create an embedded test.
Copy log4j.properties and cassandra.yaml from src/test/resources to your project's test resources.


Keyspace and CF creation
------------------------

Currently secondary indexing is automatically created for the columns of field that are annotated with the @Index annotation
if the auto table create is enabled in your dn configuration.  See the src/test/resources/META-INF/jdoconfig.xml for an example.
Note that in a production environment you SHOULD NOT set datanucleus.autoCreateSchema="true".  This uses the simple replication
strategy with a replication factor of 1.  This is ideal for rapid development on a local machine, but not for a production enironment.



Querying
--------

All queries must contain at least one == operand.  For instance, this is a valid query.

	"follower == :email && lastupdate > :max use"

This is not

	"follower == :email || follower == :email2"

Currently as of 0.7.0, Cassandra cannot support OR operations.  As a result all subtrees of OR ops in a query AST are performed independently
and the candidate results are unioned.

Ordering and Paging
-------------------

Currently in memory ordering and paging are supported.  Since cassandra will return results in any order when using an || operand in your query
you cannot guarantee order.  Therefore ordering must be used when paging is used.  Be aware that all values from 0 to your defined start index
are loaded and sorted before being ignored.  This can be quite memory intensive.  You may get better performance by modifying the range your
query runs over.


Inheritance
-----------

All inheritance requires a discriminator strategy if there more than 1 concrete class is in the inheritance tree.
This is due to a limitation of the plugin.  Note that a CF per concrete class is much less efficient than
storing all subclasses in the parent CF.  

Storing the subclasses in the parent CF requires O(2) reads.  

1. One to read the class type and instantiate the instance. O(1)
2. Read the columns and populate the object. O(1)

Storing the subclass in its own table requires O(n+1) reads where n is the number of children from the queried class

1. Recursively read all CFs in the inheritance structure until we find a result O(n)
2. Read the columns and populate the object O(1) 



Building
--------

You no longer need to manually install the cassandra depdenencies when building.  They will be installed and deployed automatically by maven.


Roadmap
-------

Change serialisation of all Serializable objects from JVM native to external library for performance increase


