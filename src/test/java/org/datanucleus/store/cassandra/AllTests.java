package org.datanucleus.store.cassandra;

import java.io.IOException;
import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
import org.datanucleus.store.cassandra.array.ArrayTests;
import org.datanucleus.store.cassandra.basic.PrimitiveTests;
import org.datanucleus.store.cassandra.collection.CollectionTests;
import org.datanucleus.store.cassandra.map.MapTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses( { ArrayTests.class, CollectionTests.class, MapTests.class,
		PrimitiveTests.class })
public class AllTests {

	
	
	

}
