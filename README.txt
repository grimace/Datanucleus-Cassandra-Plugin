This plugin supports the following operations.

* Basic CRUD
* Graph persistence of all loaded objects
* Cascading deletes of all depdendent objects
* Subclass retrieval and persistence
* Basic secondary indexing with simple terms.  && || < <= > >= and == are supported.


See all unit/integration tests for details on how to annotate objects, as well as correctly query them.

IMPLEMENTATION NOTES:

Currently secondary indexing requires a Column Family per index.  Lucandra was initially used as it allowed
greater query functionality.  However this implementation cannot be used on numeric fields due to this bug.

https://issues.apache.org/jira/browse/CASSANDRA-1235

Secondary indexing with Lucandra will be implemented after this issue is resolved and released.

BUILDING:

Building is currently not a tivial task.  We depend on Pelops, which can be downloaded here.

http://code.google.com/p/pelops/

Currently version 0.804 is supported.

You will also need to manually install the cassandra depdenencies.  The next release will manually install all
required jars during the build process.

USAGE:

All inheritance requires a descriminator strategy if there more than 1 concrete class is in the inheritance tree.
This is due to a limitation of the plugin.  Note that a CF per concrete class is much less efficient than
storing all subclasses in the parent CF.  

Storing the subclasses in the parent CF requires O(2) reads.  
1. One to read the class type and instanciate the instance. O(1)
2. Read the columns and populate the object. O(1)

Storing the subclass in its own table requires O(n+1) reads where n is the number of children from the queried class
1. Recursively read all CFs in the inheritance structure until we find a result O(n)
2. Read the columns and populate the object O(1) 

