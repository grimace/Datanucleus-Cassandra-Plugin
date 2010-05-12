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


import static org.datanucleus.store.cassandra.utils.ByteConverter.getBytes;
import static org.datanucleus.store.cassandra.utils.MetaDataUtils.getKey;

import java.util.List;

import me.prettyprint.cassandra.service.BatchMutation;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
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
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.cassandra.mutate.BatchMutationManager;

public class CassandraPersistenceHandler extends AbstractPersistenceHandler {

	private CassandraStoreManager manager;
	private BatchMutationManager batchManager;

	public CassandraPersistenceHandler(CassandraStoreManager manager) {
		this.manager = manager;
		this.batchManager = new BatchMutationManager();
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
		// do nothing, we use locate instead
		return null;
	}

	@Override
	public void insertObject(StateManager sm) {

		// just delegate to update. They both perform the same logic
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

//			List<Column> updates = new Stack<Column>();
//			List<SuperColumn> superColumns = new Stack<SuperColumn>();
//			List<Deletion> deletes = new Stack<Deletion>();
			
			ExecutionContext ec = sm.getObjectManager().getExecutionContext();

			//signal a write is about to start
			this.batchManager.beginWrite(ec, sm);
			
			CassandraInsertFieldManager manager = new CassandraInsertFieldManager(this.batchManager, sm, metaData.getTable(), getKey(sm), this.manager.getTimestamp().getTime());
			
			sm.provideFields(metaData.getAllMemberPositions(), manager);
			
			BatchMutation mutate = this.batchManager.endWrite(ec, sm);
			
			//this is the root object, perform a write operation to cassandra
			if(mutate != null){
				conn.getKeyspace().batchMutate(mutate);
			}

//			String key = getKey(sm);
//			Stack<String> columnFamilies = new Stack<String>();
//			columnFamilies.add(metaData.getTable());
//
//			// now perform the batch update
//			BatchMutation changes = new BatchMutation();
//
//			for (Column column : updates) {
//				changes.addInsertion(key, columnFamilies, column);
//			}
//			
//			for( SuperColumn superColumn: superColumns){
//				changes.addSuperInsertion(key, columnFamilies, superColumn);
//			}
//
//			for (Deletion deletion : deletes) {
//				changes.addDeletion(key, columnFamilies, deletion);
//			}

			

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	private CassandraManagedConnection getConnection(StateManager sm) {
		return (CassandraManagedConnection) this.manager.getConnection(sm
				.getObjectManager());

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

	private void populateKeys(List<Column> columns,
			AbstractClassMetaData metaData, String keyValues) {

		String[] pks = metaData.getPrimaryKeyMemberNames();

		if (pks.length != 1) {
			throw new NucleusUserException(
					"The cassandra store currently only support one pk per class");
		}

		Column column = new Column(getBytes(pks[0]), getBytes(keyValues), 0);

		columns.add(column);
	}

}