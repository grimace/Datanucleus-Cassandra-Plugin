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

package com.spidertracks.datanucleus.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.map.model.CardMap;
import com.spidertracks.datanucleus.map.model.CardMapDate;
import com.spidertracks.datanucleus.map.model.PackMap;
import com.spidertracks.datanucleus.map.model.PackMapDate;

/**
 * @author Todd Nine
 * 
 */

public class MapTest  extends CassandraTest {



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

		assertEquals(aceSpades, saved.getCards().get(aceSpades.getName()));

		assertEquals(jackHearts, saved.getCards().get(jackHearts.getName()));

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

		assertEquals(aceSpades, saved.getCards().get(aceSpades.getTime()));

		assertEquals(jackHearts, saved.getCards().get(jackHearts.getTime()));

	}


}
