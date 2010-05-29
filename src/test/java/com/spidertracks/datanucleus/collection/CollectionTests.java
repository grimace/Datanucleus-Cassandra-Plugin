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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

/**
 * @author Todd Nine
 * 
 */

public class CollectionTests extends CassandraTest  {



	@Test
	public void testBasicPeristAndLoadOneToManyCollection() throws Exception {

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
	public void testBasicPeristAndLoadBiDirectionalCollection() throws Exception {

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
		
		//saved ace spades
		Card savedAceSpades = saved.getCards().get(saved.getCards().indexOf(aceSpades));
		
		assertEquals(pack, savedAceSpades.getPack());
		
		Card savedJackHeartsSpades = saved.getCards().get(saved.getCards().indexOf(jackHearts));
		
		assertEquals(pack, savedJackHeartsSpades.getPack());

	}



}
