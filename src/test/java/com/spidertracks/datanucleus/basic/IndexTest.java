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

import static org.junit.Assert.*;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.model.Person;
import com.spidertracks.datanucleus.utils.ByteConverter;

/**
 * @author Todd Nine
 * 
 */
public class IndexTest extends CassandraTest {

	private PersistenceManager pm;
	private Person p1;
	private Person p2;
	private Person p3;
	private Person p4;
	private Person p5;

	@Before
	public void populateData() {
		p1 = new Person();
		p1.setEmail("p1@test.com");
		p1.setFirstName("firstName1");
		p1.setLastName("lastName1");

		p2 = new Person();
		p2.setEmail("p2@test.com");
		p2.setFirstName("firstName1");
		p2.setLastName("secondName1");

		p3 = new Person();
		p3.setEmail("p3@test.com");
		p3.setFirstName("firstName1");
		p3.setLastName("secondName2");

		p4 = new Person();
		p4.setEmail("p4@test.com");
		p4.setFirstName("firstName2");
		p4.setLastName("secondName2");

		p5 = new Person();
		p5.setEmail("p5@test.com");
		p5.setFirstName("firstName3");
		p5.setLastName("secondName3");

		// now persist everything

		pm = pmf.getPersistenceManager();
		Transaction tx = pm.currentTransaction();
		tx.begin();

		pm.makePersistent(p1);
		pm.makePersistent(p2);
		pm.makePersistent(p3);
		pm.makePersistent(p4);
		pm.makePersistent(p5);

	}
	

	@After
	public void deleteData() {
		//delete all our test data
			
		pm.deletePersistent(p1);
		pm.deletePersistent(p2);
		pm.deletePersistent(p3);
		pm.deletePersistent(p4);
		pm.deletePersistent(p5);
		
		Transaction tx = pm.currentTransaction();
		tx.commit();
		
	}


	@Test
	public void testIndexCreation() throws Exception {

		// now test our secondary indexes are correct
		Selector selector = Pelops.createSelector("TestPool", "Keyspace1");

		// p1, p2 and p3 all have "firstName1"
		List<Column> keys = selector.getColumnsFromRow("firstName1",
				"Person_FirstName",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);

		assertEquals(3, keys.size());

		Person[] resultPoints = new Person[] { p1, p2, p3 };

		int count = 0;

		for (Person current : resultPoints) {
			// now check our keys
			for (Column col : keys) {
				String name = ByteConverter.getString(col.getName());
				if (current.getId().toString().equals(name)) {
					count++;
					break;
				}
			}
		}

		// test we have all 3 ids as columns
		assertEquals(3, count);
		
		//results for lastname
		// p3 an p4 all have "secondName2"
		keys = selector.getColumnsFromRow("secondName2",
				"Person_LastName",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);
	
		assertEquals(2, keys.size());

		
		resultPoints = new Person[] {  p3, p4 };

		count = 0;

		for (Person current : resultPoints) {
			// now check our keys
			for (Column col : keys) {
				String name = ByteConverter.getString(col.getName());
				if (current.getId().toString().equals(name)) {
					count++;
					break;
				}
			}
		}

		// test we have all 3 ids as columns
		assertEquals(2, count);
		
		
		keys = selector.getColumnsFromRow("p5@test.com",
				"Person_email",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);
	
		assertEquals(1, keys.size());

		
		resultPoints = new Person[] {  p5 };

		count = 0;

		for (Person current : resultPoints) {
			// now check our keys
			for (Column col : keys) {
				String name = ByteConverter.getString(col.getName());
				if (current.getId().toString().equals(name)) {
					count++;
					break;
				}
			}
		}

		// test we have all 3 ids as columns
		assertEquals(1, count);
		
		

	}


	@Test
	public void testIndexDeletion() throws Exception {

		//delete object p1 p2 and p3
		Transaction trans = pm.currentTransaction();
		
		pm.deletePersistent(p1);
		pm.deletePersistent(p2);
		pm.deletePersistent(p3);
		
		
		// now test our secondary indexes are correct
		Selector selector = Pelops.createSelector("TestPool", "Keyspace1");

		// p1, p2 and p3 all have "firstName1"
		List<Column> keys = selector.getColumnsFromRow("firstName1",
				"Person_FirstName",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);

		assertEquals(0, keys.size());

		
		pm.deletePersistent(p4);
		
		//results for lastname
		// p3 an p4 all have "secondName2"
		keys = selector.getColumnsFromRow("secondName2",
				"Person_LastName",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);
	
		assertEquals(0, keys.size());

		pm.deletePersistent(p5);
		
		keys = selector.getColumnsFromRow("p5@test.com",
				"Person_email",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);
	
		assertEquals(0, keys.size());


	}

	@Test
	public void testIndexNulledValue() throws Exception {
		
		//we'll null our p3 firtName and check it's removed from the index.
		p3.setFirstName(null);
		
		pm.makePersistent(p3);
		
		Transaction trans = pm.currentTransaction();
		trans.commit();
		trans.begin();

		// now test our secondary indexes are correct
		Selector selector = Pelops.createSelector("TestPool", "Keyspace1");

		// p1, p2 and p3 all have "firstName1"
		List<Column> keys = selector.getColumnsFromRow("firstName1",
				"Person_FirstName",
				Selector.newColumnsPredicateAll(false, 100),
				ConsistencyLevel.ONE);

		assertEquals(2, keys.size());

		Person[] resultPoints = new Person[] { p1, p2};

		int count = 0;

		for (Person current : resultPoints) {
			// now check our keys
			for (Column col : keys) {
				String name = ByteConverter.getString(col.getName());
				if (current.getId().toString().equals(name)) {
					count++;
					break;
				}
			}
		}

		// test we have all 3 ids as columns
		assertEquals(2, count);
	}
	

	@Test
	public void testIndexEmptyValue() throws Exception {
		
		//we'll null our p3 firtName and check it's removed from the index.
	
		Person p6 = new Person();
		p6.setEmail("p6@test.com");
		p6.setFirstName("");
		p6.setLastName("");
		
		pm.makePersistent(p6);
		
		Transaction trans = pm.currentTransaction();
		trans.commit();
		trans.begin();


		//shouldn't have blown up, now perform a remove
		p6 = pm.getObjectById(Person.class, p6.getId());
		
		pm.deletePersistent(p6);
		
		trans.commit();
		trans.begin();
		
	}


}
