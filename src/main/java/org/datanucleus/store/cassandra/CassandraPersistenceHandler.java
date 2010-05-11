/**********************************************************************
Copyright (c) 2010 Todd Nine and others. All rights reserved.
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

import java.util.List;
import java.util.Stack;

import static org.datanucleus.store.cassandra.utils.ByteConverter.getBytes;

import me.prettyprint.cassandra.service.BatchMutation;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;

public class CassandraPersistenceHandler extends AbstractPersistenceHandler {

	private CassandraStoreManager manager;

	public CassandraPersistenceHandler(CassandraStoreManager manager) {
		this.manager = manager;
	}

	@Override
	public void close() {

	}

	@Override
	public void deleteObject(StateManager sm) {

		CassandraManagedConnection conn = null;

		try {
			// delete the whole column family for this key
			conn = getConnection(sm);
			conn.getKeyspace().remove(getKey(sm),
					getClassColumnFamily(sm.getClassMetaData()));
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	@Override
	public void fetchObject(StateManager sm, int[] fieldNumbers) {

		CassandraManagedConnection conn = null;

		try {
			// delete the whole column family for this key
			conn = getConnection(sm);
			AbstractClassMetaData metaData = sm.getClassMetaData();
			
			String key = getKey(sm);
			
		
			
			List<Column> columns = conn.getKeyspace().getSlice(key,
					getColumnParent(metaData), getSliceprediCate(metaData));

		
			populateKeys(columns, metaData, key);

			CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
					columns, metaData);
			sm.replaceFields(metaData.getAllMemberPositions(), manager);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

	}
	

	@Override
	public Object findObject(ObjectManager om, Object id) {
		//do nothing, we use locate instead
		return null;
	}


	@Override
	public void insertObject(StateManager sm) {

		//just delegate to update.  They both perform the same logic
		updateObject(sm, sm.getClassMetaData().getAllMemberPositions());
		

	}

	@Override
	public void locateObject(StateManager sm) {
		CassandraManagedConnection conn = null;

		try {
			// delete the whole column family for this key
			conn = getConnection(sm);
			AbstractClassMetaData metaData = sm.getClassMetaData();

			String key = getKey(sm);
			List<Column> columns = conn.getKeyspace().getSlice(key,
					getColumnParent(metaData), getSliceprediCate(metaData));

			if (columns == null || columns.size() == 0) {
				throw new NucleusObjectNotFoundException(String.format(
						"Couldn't find object %s with key %s", metaData
								.getName(), key));
			}
			

			populateKeys(columns, metaData, key);

			CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
					columns, metaData);
			sm.replaceFields(metaData.getAllMemberPositions(), manager);

		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (NotFoundException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

	}
	



	@Override
	public void updateObject(StateManager sm, int[] fieldNumbers) {
		this.manager.assertReadOnlyForUpdateOfObject(sm);

		 
		CassandraManagedConnection conn = null;

		try {
			// delete the whole column family for this key
			conn = getConnection(sm);
			AbstractClassMetaData metaData = sm.getClassMetaData();
			
			
			List<Column> updates = new Stack<Column>();
			List<Deletion> deletes = new Stack<Deletion>();
			
			
			
			CassandraInsertFieldManager manager = new CassandraInsertFieldManager(updates, deletes, this.manager.getTimestamp().getTime(), metaData);
			sm.provideFields(metaData.getAllMemberPositions(), manager);
			
			
			String key = getKey(sm);
			List<String> columnFamilies = new Stack<String>();
			columnFamilies.add(metaData.getTable());
			
			//now perform the batch update
			BatchMutation changes = new BatchMutation();
			
			
			for (Column column : updates) {
				changes.addInsertion(key, columnFamilies,column );
			}
			
			
			for (Deletion deletion : deletes) {
				changes.addDeletion(key, columnFamilies,deletion );
			}
			
			conn.getKeyspace().batchMutate(changes);
			
	            
		} catch (Exception e) {
			throw new NucleusDataStoreException (e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	private CassandraManagedConnection getConnection(StateManager sm) {
		return (CassandraManagedConnection) this.manager.getConnection(sm.getObjectManager());

	}

	/**
	 * Get the primary key field of this class. Allows the user to define more
	 * than one field for a PK
	 * 
	 * @param op
	 * @return
	 */
	private String getKey(StateManager sm) {

		StringBuffer buffer = new StringBuffer();

		for (int index : sm.getClassMetaData().getPKMemberPositions()) {
			buffer.append(sm.provideField(index));
		}

		return buffer.toString();
	}

	/**
	 * Get the column path to the entire class
	 * 
	 * @param metaData
	 * @return
	 */
	private ColumnPath getClassColumnFamily(AbstractClassMetaData metaData) {
		return new ColumnPath(metaData.getTable());

	}

	/**
	 * Get the column parent for the given class metadata. Matches the
	 * "table name" on the class meta data
	 * 
	 * @param op
	 * @return
	 */
	private ColumnParent getColumnParent(AbstractClassMetaData metaData) {
		return new ColumnParent(metaData.getTable());
	}

	/**
	 * Return a slice predicate that will read all of the managed columns in the
	 * class meta data and it's parent class
	 * 
	 * @param op
	 * @return
	 */
	private SlicePredicate getSliceprediCate(AbstractClassMetaData metaData) {
		// start and 0 and 0 to not limit the result set, keep the in ascending
		// order and get the number of columns from the meta data
		SliceRange sr = new SliceRange(new byte[0], new byte[0], false,
				metaData.getMemberCount());
		SlicePredicate sp = new SlicePredicate();
		sp.setSlice_range(sr);

		return sp;
	}
	
	private void populateKeys(List<Column> columns, AbstractClassMetaData metaData, String keyValues){
		
		String[] pks = metaData.getPrimaryKeyMemberNames();
		
		if(pks.length != 1){
			throw new NucleusUserException("The cassandra store currently only support one pk per class");
		}
		
		
		Column column = new Column(getBytes(pks[0]), getBytes(keyValues), 0);
		
		columns.add(column);
	}


	
	
}