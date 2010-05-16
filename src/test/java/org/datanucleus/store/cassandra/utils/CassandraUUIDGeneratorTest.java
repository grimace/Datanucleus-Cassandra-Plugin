/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package org.datanucleus.store.cassandra.utils;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.UUID;

import org.datanucleus.store.cassandra.identity.CassandraUUIDGenerator;

import org.junit.Test;

/**
 * @author Todd Nine
 *
 */
public class CassandraUUIDGeneratorTest {

	/**
	 * Test method for {@link org.datanucleus.store.cassandra.identity.CassandraUUIDGenerator#CassandraUUIDGenerator(java.lang.String, java.util.Properties)}.
	 */
	@Test
	public void testCassandraUUIDGenerator() {
		
		//smoke test
		new CassandraUUIDGenerator(null, null);
		
		
	}

	/**
	 * Test method for {@link org.datanucleus.store.cassandra.identity.CassandraUUIDGenerator#reserveBlock(long)}.
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
