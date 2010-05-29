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

package com.spidertracks.datanucleus.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.array.model.CardArray;
import com.spidertracks.datanucleus.array.model.PackArray;

/**
 * @author Todd Nine
 * 
 */

public class ArrayTests extends CassandraTest {


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
