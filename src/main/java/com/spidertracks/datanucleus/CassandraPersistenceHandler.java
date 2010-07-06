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

Contributors : Pedro Gomes and Universidade do Minho.
    		 : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.*;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.wyki.cassandra.pelops.KeyDeletor;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

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
	public void deleteObject(ObjectProvider op) {
		try {

			String poolName = manager.getPoolName();
			String keySpace = manager.getKeyspace();

			String key = getRowKey(op);

			KeyDeletor deletor = Pelops.createKeyDeletor(poolName, keySpace);

			deletor.deleteColumnFamily(key, getColumnFamily(op
					.getClassMetaData()), ConsistencyLevel.ONE);

			// delete our secondary index as well

			AbstractClassMetaData metaData = op.getClassMetaData();

			int[] fields = metaData.getAllMemberPositions();

			Mutator mutator = Pelops.createMutator(poolName, keySpace);

			for (int current : fields) {
				AbstractMemberMetaData fieldMetaData = metaData
						.getMetaDataForManagedMemberAtAbsolutePosition(current);

				String secondaryCfName = getIndexName(metaData, fieldMetaData);

				// nothing to index
				if (secondaryCfName == null) {
					continue;
				}

				// here we have the field value
				Object value = op.provideField(current);

				if (value == null) {
					continue;
				}

				// convert it to a string so we can key it
				String keyValue = convertToRowKey(op.getExecutionContext(), value);

				//no value to remove
				if(keyValue == null || keyValue.length() == 0){
					continue;
				}
				
				// blitz the reverse index
				mutator.deleteColumn(keyValue, secondaryCfName, key);

			}

			mutator.execute(ConsistencyLevel.ONE);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void fetchObject(ObjectProvider op, int[] fieldNumbers) {
		try {
			AbstractClassMetaData metaData = op.getClassMetaData();

			String key = getRowKey(op);

			Selector selector = Pelops.createSelector(manager.getPoolName(),
					manager.getKeyspace());

			List<Column> columns = selector.getColumnsFromRow(key,
					getColumnFamily(metaData), getFetchColumnList(metaData,
							fieldNumbers), DEFAULT);

			//nothing to do
			if (columns == null || columns.size() == 0) {
				return;
			}

			CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
					columns, op);

			op.replaceFields(fieldNumbers, manager);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

	}

	@Override
	public Object findObject(ExecutionContext ectx, Object id) {
		// do nothing, we use locate instead
		return null;
	}

	@Override
	public void insertObject(ObjectProvider op) {
		// just delegate to update. They both perform the same logic
		updateObject(op, op.getClassMetaData().getAllMemberPositions());

	}

	@Override
	public void locateObject(ObjectProvider op) {
		fetchObject(op, op.getClassMetaData().getAllMemberPositions());

	}

	@Override
	public void updateObject(ObjectProvider op, int[] fieldNumbers) {

		this.manager.assertReadOnlyForUpdateOfObject(op);

		AbstractClassMetaData metaData = op.getClassMetaData();

		ExecutionContext ec = op.getExecutionContext();

		Mutator mutator = Pelops.createMutator(manager.getPoolName(), manager
				.getKeyspace());

		// signal a write is about to start
		this.batchManager.beginWrite(ec, mutator);

		String key = getRowKey(op);

		// Write our all our primary object data
		CassandraInsertFieldManager manager = new CassandraInsertFieldManager(
				mutator, op, getColumnFamily(metaData), key);

		op.provideFields(metaData.getAllMemberPositions(), manager);

		try {

			this.batchManager.endWrite(ec, DEFAULT);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

	}


}