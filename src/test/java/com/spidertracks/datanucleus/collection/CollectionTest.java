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

package com.spidertracks.datanucleus.collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.junit.Test;

import com.eaio.uuid.UUID;
import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

/**
 * @author Todd Nine
 * 
 */

public class CollectionTest extends CassandraTest {

	@Test
	public void testBasicPeristAndLoadOneToManyCollection() throws Exception {

		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.addCard(aceSpades);

		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.addCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		Pack saved = pmf.getPersistenceManager().getObjectById(Pack.class,
				pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards().contains(aceSpades));

		assertTrue(saved.getCards().contains(jackHearts));

	}

	@Test
	public void testBasicPeristAndLoadBiDirectionalCollection()
			throws Exception {

		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.addCard(aceSpades);

		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.addCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		Pack saved = pmf.getPersistenceManager().getObjectById(Pack.class,
				pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards().contains(aceSpades));

		assertTrue(saved.getCards().contains(jackHearts));

		// saved ace spades
		Card savedAceSpades = saved.getCards().get(
				saved.getCards().indexOf(aceSpades));

		assertEquals(pack, savedAceSpades.getPack());

		Card savedJackHeartsSpades = saved.getCards().get(
				saved.getCards().indexOf(jackHearts));

		assertEquals(pack, savedJackHeartsSpades.getPack());

	}

	@Test
	public void testSaveAndLoadSamePmf() throws Exception {

		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.addCard(aceSpades);

		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.addCard(jackHearts);

		PersistenceManager pm = pmf.getPersistenceManager();

		pm.makePersistent(pack);

		Pack saved = pm.getObjectById(Pack.class, pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards().contains(aceSpades));

		assertTrue(saved.getCards().contains(jackHearts));

		// saved ace spades
		Card savedAceSpades = saved.getCards().get(
				saved.getCards().indexOf(aceSpades));

		assertEquals(pack, savedAceSpades.getPack());

		Card savedJackHeartsSpades = saved.getCards().get(
				saved.getCards().indexOf(jackHearts));

		assertEquals(pack, savedJackHeartsSpades.getPack());
	}

	/**
	 * Tests that when an empty collection is persisted then lazy loaded, no
	 * exceptions occur.
	 * 
	 * @throws Exception
	 */
	@Test
	public void emptyCollectionDoesNotCauseException() throws Exception {

		Pack pack = new Pack();

		PersistenceManager pm = pmf.getPersistenceManager();

		pm.makePersistent(pack);

		PersistenceManager pm2 = pmf.getPersistenceManager();

		Transaction tx = pm2.currentTransaction();
		tx.begin();
		Pack saved = pm2.getObjectById(Pack.class, pack.getId());

		List<Card> emptyList = saved.getCards();

		tx.commit();

		assertNull(emptyList);

	}

	@Test
	public void testDeleteDependencies() throws Exception {

		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.addCard(aceSpades);

		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.addCard(jackHearts);

		pmf.getPersistenceManager().makePersistent(pack);

		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction trans = pm.currentTransaction();
		trans.begin();

		Pack saved = pm.getObjectById(Pack.class, pack.getId());

		assertEquals(pack, saved);

		assertNotNull(saved.getCards());

		assertTrue(saved.getCards().contains(aceSpades));

		assertTrue(saved.getCards().contains(jackHearts));

		// saved ace spades
		Card savedAceSpades = saved.getCards().get(
				saved.getCards().indexOf(aceSpades));

		assertEquals(pack, savedAceSpades.getPack());

		Card savedJackHearts = saved.getCards().get(
				saved.getCards().indexOf(jackHearts));

		assertEquals(pack, savedJackHearts.getPack());
		
		UUID packId = pack.getId();
		UUID aceId = aceSpades.getId();
		UUID jackId = jackHearts.getId();

		// now perform a delete and ensure that everything is deleted

		pm.deletePersistent(saved);
		trans.commit();

		boolean deleted = false;

		try {
			pmf.getPersistenceManager().getObjectById(Pack.class,packId);
		} catch (JDODataStoreException n) {
			deleted = n.getCause() instanceof NucleusObjectNotFoundException;
		}

		assertTrue(deleted);

		deleted = false;

		// now check the cards are gone as well
		try {
			pmf.getPersistenceManager().getObjectById(Card.class,
					aceId);
		} catch (JDODataStoreException n) {
			deleted = n.getCause() instanceof NucleusObjectNotFoundException;
		}

		assertTrue(deleted);

		deleted = false;
		try {
			pmf.getPersistenceManager().getObjectById(Card.class,
					jackId);

		} catch (JDODataStoreException n) {
			deleted = n.getCause() instanceof NucleusObjectNotFoundException;
		}

		assertTrue(deleted);

		deleted = false;
	}

}
