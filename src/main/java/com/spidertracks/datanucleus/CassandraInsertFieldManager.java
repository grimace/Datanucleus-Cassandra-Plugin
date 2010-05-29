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
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.ByteConverter.getBytes;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.getKey;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnPath;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;

import com.spidertracks.datanucleus.mutate.BatchMutationManager;

/**
 * @author Todd Nine
 * 
 */
public class CassandraInsertFieldManager extends CassandraFieldManager {

	// private List<Column> updates;
	// private List<SuperColumn> superColumns;
	// private List<Deletion> deletes;
	private ExecutionContext context;
	private BatchMutationManager manager;
	private AbstractClassMetaData metaData;
	private StateManager stateManager;
	private String columnFamily;
	private String rowKey;
	private long timestamp;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(BatchMutationManager manager,
			StateManager stateManager, String tableName, String rowKey,
			long updateTimestamp) {
		super();

		this.manager = manager;
		this.stateManager = stateManager;
		this.metaData = stateManager.getClassMetaData();
		this.context = stateManager.getObjectProvider().getExecutionContext();

		// this.updates = updates;
		// this.superColumns = superColumns;
		// this.deletes = deletes;
		this.columnFamily = tableName;
		this.rowKey = rowKey;
		this.timestamp = updateTimestamp;

	}

	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);

			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					new byte[] { value }, timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}



	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		try {


			String columnName = getColumnName(metaData, fieldNumber);

			// delete operation
			if (value == null) {

				this.manager.addDelete(context, columnFamily, rowKey,
						columnName, timestamp);

				return;

			}

			ObjectProvider op = stateManager.getObjectProvider();

			ClassLoaderResolver clr = context.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = metaData.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
			int relationType = fieldMetaData.getRelationType(clr);

			// check if this is a relationship

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {
				// Persistable object - persist the related object and store the
				// identity in the cell

				if (fieldMetaData.isEmbedded()) {

						throw new NucleusDataStoreException(
								"Embedded objects are unsupported.  Mark the object as persistent and use a serializable class instead");

				
				}

				Object persisted = context.persistObjectInternal(value, op, -1,
						StateManager.PC);

				// TODO add this data to the supercolumn info

				this.manager.addColumn(context, columnFamily, rowKey,
						columnName, getBytes(getKey(this.context, persisted)),
						timestamp);

				return;
			}

			if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {
				// Collection/Map/Array

				if (fieldMetaData.hasCollection()) {

					List<String> serializedKeys = new ArrayList<String>(
							((Collection<?>) value).size());

					Object persisted = null;

					for (Object element : (Collection<?>) value) {
						// persist the object
						persisted = context.persistObjectInternal(element, op,
								-1, StateManager.PC);

						serializedKeys.add(getKey(this.context, persisted));

					}

					this.manager.addColumn(context, columnFamily, rowKey,
							columnName, getBytes(serializedKeys), timestamp);
					
					return;

				} else if (fieldMetaData.hasMap()) {

					ApiAdapter adapter = context.getApiAdapter();

					Map<?,?> map = ((Map<?,?>) value);
					
					Map<Object, Object> serializedMap = new HashMap<Object, Object>(map.size());
					
					//serialized values to store per item
					Object serializedKey = null;
					Object serializedValue = null;

					//value set by the user in the  map
					Object mapValue = null;

					
					//pointer to what we persisted
					Object persisted = null;

					// get each element and persist it.
					for (Object mapKey : map.keySet()) {

						mapValue = map.get(mapKey);

						// handle the case if our key is a persistent class itself
						if (adapter.isPersistable(mapKey)) {

							persisted = context.persistObjectInternal(mapKey,
									op, -1, StateManager.PC);

							serializedKey = getKey(this.context, persisted);
						} else {
							serializedKey = mapKey;
						}

						// persist the value if it can be persisted
						if (adapter.isPersistable(mapValue)) {

							persisted = context.persistObjectInternal(mapValue,
									op, -1, StateManager.PC);

							serializedValue = getKey(this.context, persisted);
						} else {
							serializedKey = mapValue;
						}

						serializedMap.put(serializedKey, serializedValue);

					}

					this.manager.addColumn(context, columnFamily, rowKey,
							columnName, getBytes(serializedMap), timestamp);
					
					return;

				} else if (fieldMetaData.hasArray()) {

					List<String> serializedKeys = new ArrayList<String>(Array
							.getLength(value));

					Object persisted = null;

					for (int i = 0; i < Array.getLength(value); i++) {
						// persist the object
						persisted = context.persistObjectInternal(Array.get(
								value, i), op, -1, StateManager.PC);

						serializedKeys.add(getKey(this.context, persisted));
					}

					this.manager.addColumn(context, columnFamily, rowKey,
							columnName, getBytes(serializedKeys), timestamp);

				}

				return;
			}

			// default case where we persist raw objects
			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {
				manager.addDelete(context, columnFamily, rowKey, columnName,
						fieldNumber);
				return;
			}

			manager.addColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	protected ColumnPath getColumnPath(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {
		ColumnPath columnPath = new ColumnPath(metaData.getTable());
		columnPath.setColumn(getBytes(getColumnName(metaData,
				absoluteFieldNumber)));

		return columnPath;
	}

}
