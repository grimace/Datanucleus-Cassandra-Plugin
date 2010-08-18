This plugin supports the following operations.

* Basic CRUD
* Graph persistence of all loaded objects
* Cascading deletes of all depdendent objects
* Subclass retrieval and persistence
* Basic secondary indexing with simple terms.  && || < <= > >= and == are supported.


See all unit/integration tests for details on how to annotate objects, as well as correctly query them.

NOTES:

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

