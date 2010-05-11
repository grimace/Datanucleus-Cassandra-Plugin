/**
 * 
 */
package org.datanucleus.store.cassandra.utils;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.UUID;
import org.datanucleus.store.cassandra.utils.CassandraUUIDGenerator;

import org.junit.Test;

/**
 * @author Todd Nine
 *
 */
public class CassandraUUIDGeneratorTest {

	/**
	 * Test method for {@link org.datanucleus.store.cassandra.utils.CassandraUUIDGenerator#CassandraUUIDGenerator(java.lang.String, java.util.Properties)}.
	 */
	@Test
	public void testCassandraUUIDGenerator() {
		
		//smoke test
		new CassandraUUIDGenerator(null, null);
		
		
	}

	/**
	 * Test method for {@link org.datanucleus.store.cassandra.utils.CassandraUUIDGenerator#reserveBlock(long)}.
	 */
	@Test
	public void testReserveBlockLong() {
		CassandraUUIDGenerator generator = new CassandraUUIDGenerator(null, null);
		
		int keys = 300;
		
		generator.reserveBlock(keys);
		
		
		
		HashMap<UUID, Boolean> ids = new HashMap<UUID, Boolean>(keys);
		
		for(int i = 0; i < keys; i ++){
			UUID value = (UUID) generator.next();
			
			if(ids.containsKey(value)){
				fail(String.format("Key %s was generated twice", value));
			}
			
			ids.put(value, false);
		}
		
		
		
		
		
		
	}

}
