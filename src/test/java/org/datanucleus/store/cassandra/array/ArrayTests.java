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

package org.datanucleus.store.cassandra.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.CassandraServer;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
import org.datanucleus.store.cassandra.array.model.CardArray;
import org.datanucleus.store.cassandra.array.model.PackArray;
import org.datanucleus.store.cassandra.basic.model.DateEntity;
import org.datanucleus.store.cassandra.basic.model.PrimitiveObject;
import org.datanucleus.store.cassandra.collection.model.Card;
import org.datanucleus.store.cassandra.collection.model.Pack;
import org.datanucleus.store.cassandra.map.model.CardMap;
import org.datanucleus.store.cassandra.map.model.CardMapDate;
import org.datanucleus.store.cassandra.map.model.PackMap;
import org.datanucleus.store.cassandra.map.model.PackMapDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Todd Nine
 * 
 */

public class ArrayTests {

	
	private static PersistenceManagerFactory pmf;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException,
			InterruptedException {
		
		CassandraServer.INSTANCE.start();
		
		 pmf = JDOHelper
			.getPersistenceManagerFactory("Test");
	}

	@AfterClass
	public static void teardown() throws IOException {
	}



	@Test
	public void testBasicPeristAndLoadOneToManyArray() throws Exception {


		PackArray pack = new PackArray();

		CardArray aceSpades = new CardArray();
		aceSpades.setName("Ace of Spades");
		pack.getCards()[0] = (aceSpades);

		CardArray jackHearts = new CardArray();
		jackHearts.setName("Jack of Hearts");
		pack.getCards()[1] = jackHearts;

		pmf.getPersistenceManager().makePersistent(pack);

		PackArray saved = pmf.getPersistenceManager().getObjectById(
				PackArray.class, pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards()[0].equals(aceSpades));

		assertTrue(saved.getCards()[1].equals(jackHearts));

	}
}
