package org.datanucleus.store.cassandra;

import me.prettyprint.cassandra.testutils.CassandraServer;

import org.datanucleus.store.cassandra.array.ArrayTests;
import org.datanucleus.store.cassandra.basic.PrimitiveTests;
import org.datanucleus.store.cassandra.collection.CollectionTests;
import org.datanucleus.store.cassandra.map.MapTests;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses( { ArrayTests.class, CollectionTests.class, MapTests.class,
		PrimitiveTests.class })
public class AllTests {
	
	@AfterClass
	public static void cleanUp(){
		CassandraServer.INSTANCE.stop();
	}

	
	
	

}
