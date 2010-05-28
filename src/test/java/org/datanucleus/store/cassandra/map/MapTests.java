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

package org.datanucleus.store.cassandra.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.CassandraServer;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
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

public class MapTests {

	
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
	public void testBasicPeristAndLoadOneToManyMap() throws Exception {

		

		PackMap pack = new PackMap();

		CardMap aceSpades = new CardMap();
		aceSpades.setName("Ace of Spades");
		pack.AddCard(aceSpades);

		CardMap jackHearts = new CardMap();
		jackHearts.setName("Jack of Hearts");
		pack.AddCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		PackMap saved = pmf.getPersistenceManager().getObjectById(
				PackMap.class, pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertEquals(saved.getCards().get(aceSpades.getName()), aceSpades);

		assertEquals(saved.getCards().get(jackHearts.getName()), jackHearts);

	}
	

	@Test
	public void testBasicPeristAndLoadOneToManyMapByDate() throws Exception {

		PackMapDate pack = new PackMapDate();

		CardMapDate aceSpades = new CardMapDate(2010,05,01);
		aceSpades.setName("Ace of Spades");
		pack.AddCard(aceSpades);

		CardMapDate jackHearts = new CardMapDate(2010,05,02);
		jackHearts.setName("Jack of Hearts");
		pack.AddCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		PackMapDate saved = pmf.getPersistenceManager().getObjectById(
				PackMapDate.class, pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertEquals(saved.getCards().get(aceSpades.getTime()), aceSpades);

		assertEquals(saved.getCards().get(jackHearts.getTime()), jackHearts);

	}


}
