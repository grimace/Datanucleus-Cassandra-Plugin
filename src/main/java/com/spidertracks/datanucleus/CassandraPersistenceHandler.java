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
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getKey;

import java.util.List;

import me.prettyprint.cassandra.service.BatchMutation;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;

import com.spidertracks.datanucleus.mutate.BatchMutationManager;

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
			conn.getKeyspace().remove(getKey(sm), getClassColumnFamily(sm.getClassMetaData()));
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

//			ExecutionContext ec = sm.getObjectProvider().getExecutionContext();
//
//			// delete the whole column family for this key
//			// signal a write is about to start
//			this.batchManager.beginWrite(ec, sm);
//
//			this.batchManager.addDelete(ec, getColumnFamily(sm
//					.getClassMetaData()), getKey(sm));
//
//			BatchMutation mutate = this.batchManager.endWrite(ec, sm);
//
//			// this is the root object, perform a write operation to cassandra
//			if (mutate != null) {
//
//				CassandraManagedConnection conn = null;
//				try {
//					conn = getConnection(sm);
//					conn.getKeyspace().batchMutate(mutate);
//				} catch (Exception e) {
//					throw new NucleusDataStoreException(e.getMessage(), e);
//				} finally {
//					if (conn != null) {
//						conn.close();
//					}
//				}
//
//			}

		
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

			CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
					columns, sm);

			sm.replaceFields(fieldNumbers, manager);

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

		fetchObject(sm, sm.getClassMetaData().getAllMemberPositions());

	}

	@Override
	public void updateObject(StateManager sm, int[] fieldNumbers) {
		this.manager.assertReadOnlyForUpdateOfObject(sm);

		AbstractClassMetaData metaData = sm.getClassMetaData();

		// List<Column> updates = new Stack<Column>();
		// List<SuperColumn> superColumns = new Stack<SuperColumn>();
		// List<Deletion> deletes = new Stack<Deletion>();

		ExecutionContext ec = sm.getObjectManager().getExecutionContext();

		// signal a write is about to start
		this.batchManager.beginWrite(ec, sm);

		CassandraInsertFieldManager manager = new CassandraInsertFieldManager(
				this.batchManager, sm, getColumnFamily(metaData), getKey(sm),
				this.manager.getTimestamp().getTime());

		sm.provideFields(metaData.getAllMemberPositions(), manager);

		BatchMutation mutate = this.batchManager.endWrite(ec, sm);

		// this is the root object, perform a write operation to cassandra
		if (mutate != null) {

			CassandraManagedConnection conn = null;
			try {
				conn = getConnection(sm);
				conn.getKeyspace().batchMutate(mutate);
			} catch (Exception e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			} finally {
				if (conn != null) {
					conn.close();
				}
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
		return new ColumnPath(getColumnFamily(metaData));

	}

	/**
	 * Get the column parent for the given class metadata. Matches the
	 * "table name" on the class meta data
	 * 
	 * @param op
	 * @return
	 */
	private ColumnParent getColumnParent(AbstractClassMetaData metaData) {
		return new ColumnParent(getColumnFamily(metaData));
	}

	/**
	 * Get the name of the column family. Uses table name, if one doesn't exist,
	 * it uses the simple name of the class
	 * 
	 * @param metaData
	 * @return
	 */
	private String getColumnFamily(AbstractClassMetaData metaData) {
		String name = metaData.getTable();

		if (name == null) {
			name = metaData.getName();
		}

		return name;
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

}