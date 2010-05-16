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

package org.datanucleus.store.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
import org.datanucleus.store.cassandra.model.Card;
import org.datanucleus.store.cassandra.model.CardArray;
import org.datanucleus.store.cassandra.model.CardMap;
import org.datanucleus.store.cassandra.model.DateEntity;
import org.datanucleus.store.cassandra.model.Pack;
import org.datanucleus.store.cassandra.model.PackArray;
import org.datanucleus.store.cassandra.model.PackMap;
import org.datanucleus.store.cassandra.model.PrimitiveObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Todd Nine
 * 
 */

public class PersistTests {

	private static EmbeddedServerHelper embedded;

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
		embedded = new EmbeddedServerHelper();
		embedded.setup();
	}

	@AfterClass
	public static void teardown() throws IOException {
		embedded.teardown();
	}

	@Test
	public void testBasicPerist() throws Exception {

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");
		PersistenceManager pm = pmf.getPersistenceManager();

		PrimitiveObject objectOne = new PrimitiveObject();

		// now save our object
		pm.makePersistent(objectOne);
	}

	@Test
	public void testBasicPeristAndLoad() throws Exception {

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");
		PersistenceManager pm = pmf.getPersistenceManager();

		PrimitiveObject object = new PrimitiveObject();
		object.setTestByte((byte) 0xf1);
		object.setTestBool(true);
		object.setTestChar('t');
		object.setTestDouble(100.10);
		object.setTestFloat((float) 200.20);
		object.setTestInt(40);
		object.setTestLong(200);
		object.setTestShort((short) 5);
		object.setTestString("foobar");

		// now save our object
		pm.makePersistent(object);

		// don't want it to come from the cache, get a new pm
		PersistenceManager pm2 = pmf.getPersistenceManager();

		// now retrieve a copy
		PrimitiveObject stored = (PrimitiveObject) pm2.getObjectById(
				PrimitiveObject.class, object.getId());

		// make sure they're not the same instance, we want a new one from the
		// data source
		assertFalse(object == stored);

		assertEquals(object.getId(), stored.getId());

		assertEquals(object.getTestByte(), stored.getTestByte());

		assertEquals(object.isTestBool(), stored.isTestBool());

		assertEquals(object.getTestChar(), stored.getTestChar());

		assertEquals(object.getTestDouble(), stored.getTestDouble(), 0);

		assertEquals(object.getTestFloat(), stored.getTestFloat(), 0);

		assertEquals(object.getTestInt(), stored.getTestInt());

		assertEquals(object.getTestLong(), stored.getTestLong());

		assertEquals(object.getTestString(), stored.getTestString());

	}

	@Test
	public void testBasicPeristAndLoadOneToManyCollection() throws Exception {

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");

		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.AddCard(aceSpades);

		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.AddCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		Pack saved = pmf.getPersistenceManager().getObjectById(Pack.class,
				pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards().contains(aceSpades));

		assertTrue(saved.getCards().contains(jackHearts));

	}

	@Test
	public void testBasicPeristAndLoadOneToManyArray() throws Exception {

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");

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

	@Test
	public void testBasicPeristAndLoadOneToManyMap() throws Exception {

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");

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

		assertEquals(saved.getCards().get(jackHearts.getName()), aceSpades);

	}

	@Test
	public void testDateEntity() throws Exception {
		DateEntity entity = new DateEntity();
		entity.setName("Test Entity");
		

		PersistenceManagerFactory pmf = JDOHelper
				.getPersistenceManagerFactory("Test");

		pmf.getPersistenceManager().makePersistent(entity);
		
		
		DateEntity saved = pmf.getPersistenceManager().getObjectById(DateEntity.class,entity.getId());

		assertEquals(entity, saved);
	}

	/**
	 * Tests an object is serialized as bytes properly
	 */
	@Test
	public void testSerializableObject() {
		fail("unimplemented");
	}

}
