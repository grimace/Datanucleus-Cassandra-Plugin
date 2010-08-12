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
package com.spidertracks.datanucleus.basic.inheritance;

import static org.junit.Assert.*;

import javax.jdo.PersistenceManager;

import org.junit.Ignore;
import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;

/**
 * Tests for 2 objects that are bi-directionally dependent and default fetch group deleting properly
 * 
 * @author Todd Nine
 * 
 */
public class InheritanceTest extends CassandraTest {




	/**
	 * We should never hit 30 seconds unless we're stuck in endless recursion
	 * @throws Exception
	 */
	@Test
	@Ignore("Should be retreiving subclass.  See CassandraPersistenceHandler.fetchObject and FetchFieldManager for comments ")
	public void testQueryChildReturnsSubclass() throws Exception {
		
		GrandChildOne first = new GrandChildOne();
		first.setChildField("cf-gc1");
		first.setGrandChildOneField("gcf-gc1");
		first.setParentField("pf-gc1");
		
		GrandChildTwo second = new GrandChildTwo();
		first.setChildField("cf-gc2");
		first.setGrandChildOneField("gcf-gc2");
		first.setParentField("pf-gc2");
		
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.makePersistent(first);
		pm.makePersistent(second);
		
		//now retrieve to instances of "child" should return 2 subclasses
		
		Child savedFirst = pm.getObjectById(Child.class, first.getId());
	
		assertEquals(first, savedFirst);
		
		Child savedSecond = pm.getObjectById(Child.class, second.getId());
		
		assertEquals(second, savedSecond);
		
	
		
		
		
	}


}
