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
package org.datanucleus.store.cassandra;

import static org.datanucleus.store.cassandra.utils.ByteConverter.getBytes;
import static org.datanucleus.store.cassandra.utils.MetaDataUtils.*;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnPath;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.cassandra.mutate.BatchMutationManager;

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

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					new byte[] { value }, timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	// these values can be deleted, so we'll want to handle deletes if they're
	// null

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		try {

			// we don't serialize keys here
			if (isKey(metaData, fieldNumber)) {
				return;
			}

			// TODO make cascading saves happen
			// return;

			String columnName = getColumnName(metaData, fieldNumber);

			// delete operation
			if (value == null) {

				this.manager.AddDelete(context, columnFamily, rowKey,
						columnName, timestamp);

				return;

			}

			ObjectProvider op = stateManager.getObjectProvider();

			ClassLoaderResolver clr = context.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = metaData
					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
			int relationType = fieldMetaData.getRelationType(clr);

			// check if this is a relationship

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {
				// Persistable object - persist the related object and store the
				// identity in the cell

				if (fieldMetaData.isEmbedded()) {
					// TODO Handle embedded objects
					throw new NucleusDataStoreException(
							"Embedded objects are currently unimplemented.");
				}

				Object persisted = context.persistObjectInternal(value, op, -1,
						StateManager.PC);

				// TODO add this data to the supercolumn info

				this.manager.AddColumn(context, columnFamily, rowKey,
						columnName, getBytes(getKey(this.context, persisted)),
						timestamp);

				return;
				// add it to a super column on this object
			}

			if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {
				// Collection/Map/Array

				// this.manager.AddSuperColumn(context, columnFamily, rowKey,
				// columnName, "c", getBytes(valueId), timestamp);

				if (fieldMetaData.hasCollection()) {

					int index = 0;

					for (Object element : (Collection) value) {
						// persist the object
						Object persisted = context.persistObjectInternal(
								element, op, -1, StateManager.PC);

						this.manager.AddSuperColumn(context, columnFamily,
								rowKey, columnName, String.valueOf(index),
								getBytes(getKey(this.context, persisted)),
								timestamp);

						index++;
					}

				} else if (fieldMetaData.hasMap()) {

					Map map = ((Map) value);
					// get each element and persist it.
					for (Object key : map.keySet()) {

						// now we're persisted, create the supercolumn map
						Object persisted = context.persistObjectInternal(map
								.get(key), op, -1, StateManager.PC);
						
						this.manager
								.AddSuperColumn(context, columnFamily, rowKey,
										columnName, convertToKey(context, key),
										getBytes(getKey(context, persisted)),
										timestamp);

					}

				} else if (fieldMetaData.hasArray()) {

					for (int i = 0; i < Array.getLength(value); i++) {
						// persist the object
						Object persisted = context.persistObjectInternal(Array
								.get(value, i), op, -1, StateManager.PC);

						this.manager.AddSuperColumn(context, columnFamily,
								rowKey, columnName, String.valueOf(i),
								getBytes(getKey(this.context, persisted)),
								timestamp);

					}

				}

				return;
			}

			// default case where we persist raw objects
			manager.AddColumn(context, columnName, rowKey, columnName,
					getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {

			if (isKey(metaData, fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {
				manager.AddDelete(context, columnFamily, rowKey, columnName,
						fieldNumber);
				return;
			}

			manager.AddColumn(context, columnFamily, rowKey, columnName,
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
