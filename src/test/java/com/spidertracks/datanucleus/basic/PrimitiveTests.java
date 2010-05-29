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

package com.spidertracks.datanucleus.basic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;

import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.converter.EnumConverter;
import com.spidertracks.datanucleus.basic.model.EmbeddedObject;
import com.spidertracks.datanucleus.basic.model.EnumEntity;
import com.spidertracks.datanucleus.basic.model.EnumValues;
import com.spidertracks.datanucleus.basic.model.PrimitiveObject;
import com.spidertracks.datanucleus.basic.model.UnitData;
import com.spidertracks.datanucleus.basic.model.UnitDataKey;

/**
 * @author Todd Nine
 * 
 */

public class PrimitiveTests  extends CassandraTest  {

	



	@Test
	public void testBasicPeristAndLoad() throws Exception {

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

	/**
	 * Tests an object is serialized as bytes properly
	 */
	@Test
	public void testCompositeKey() {
		
		UnitData key = new UnitData();
		key.setCreatedDate(Calendar.getInstance().getTime());
		
		key.setUnitId("123456");
		
		PersistenceManager pm = pmf.getPersistenceManager();
		
		pm.makePersistent(key);
		
		PersistenceManager pm2 = pmf.getPersistenceManager();

		// now retrieve a copy
		UnitData stored = (UnitData) pm2.getObjectById(UnitData.class, new UnitDataKey(key.getCreatedDate(), key.getUnitId()).toString());
		
		assertEquals(key, stored);
		
	

		
		

	}
	
	/**
	 * Tests an object is serialized as bytes properly
	 */
	@Test
	public void testByteArray() {
		
		UnitData key = new UnitData();
		key.setCreatedDate(Calendar.getInstance().getTime());
		
		key.setUnitId("123456");
		
		key.setData(new byte[]{ 0x0F, 0x0A});
		
		PersistenceManager pm = pmf.getPersistenceManager();
		
		pm.makePersistent(key);
		
		PersistenceManager pm2 = pmf.getPersistenceManager();

		// now retrieve a copy
		UnitData stored = (UnitData) pm2.getObjectById(UnitData.class, new UnitDataKey(key.getCreatedDate(), key.getUnitId()).toString());
		
		assertEquals(key, stored);
		
		assertArrayEquals(  key.getData(), stored.getData());
		
	

		
		

	}
	
	/**
	 * Tests an object is serialized as bytes properly
	 */
	@Test(expected=JDOException.class)
	public void testEmbeddedObject() {
		
		EmbeddedObject object = new EmbeddedObject();
		
		pmf.getPersistenceManager().makePersistent(object);
		
	

		
		

	}

	/**
	 * Tests an object is serialized as string types properly
	 */
	public void testObjectToStringConverterOnObject() {
		
		EnumEntity saved = new EnumEntity();
		
		saved.setFirst(EnumValues.ONE);
		
		saved.setSecond(EnumValues.TWO);
		
		//check our converter has never been invoked
		assertEquals(0, EnumConverter.getFromCount());
		
		assertEquals(0, EnumConverter.getToCount());
		
		pmf.getPersistenceManager().makePersistent(saved);
		
		EnumEntity returned = pmf.getPersistenceManager().getObjectById(EnumEntity.class, saved.getId());
		
		assertEquals(saved, returned);
		
		assertEquals(saved.getFirst(), returned.getFirst());
		
		assertEquals(saved.getSecond(), returned.getSecond());
		
		//now check our counters are correct  If they aren't the user's converter plugins weren't invoked
		assertTrue(EnumConverter.getFromCount() >= 2);
		
		assertTrue(EnumConverter.getToCount() >= 2);
	

	}

}
