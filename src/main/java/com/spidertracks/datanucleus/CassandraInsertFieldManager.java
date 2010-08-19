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

import static com.spidertracks.datanucleus.IndexPersistenceHandler.indexField;
import static com.spidertracks.datanucleus.IndexPersistenceHandler.removeIndex;
import static com.spidertracks.datanucleus.utils.ByteConverter.getBytes;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnName;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.types.ObjectStringConverter;
import org.wyki.cassandra.pelops.Mutator;

/**
 * @author Todd Nine
 * 
 */
public class CassandraInsertFieldManager extends AbstractFieldManager {

	private ExecutionContext context;
	private Mutator mutator;
	private AbstractClassMetaData metaData;
	private ObjectProvider objectProvider;
	private String columnFamily;
	private String key;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(Mutator mutator, ObjectProvider op,
			String columnFamily, String key) {
		super();

		this.mutator = mutator;
		this.objectProvider = op;
		this.metaData = op.getClassMetaData();
		this.context = op.getExecutionContext();
		this.columnFamily = columnFamily;
		this.key = key;

	}

	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			mutator
					.writeColumn(key, columnFamily, mutator.newColumn(
							getColumnName(metaData, fieldNumber),
							new byte[] { value }));
			indexField(fieldNumber, value, objectProvider, mutator);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					getColumnName(metaData, fieldNumber), getBytes(value)));
			indexField(fieldNumber, value, objectProvider, mutator);
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);

			// delete operation
			if (value == null) {
				// TODO TN we need a way to update secondary indexing if this
				// field was deleted.
				// how can we get the previous value? Only loading the current
				// value
				// then removing will work
				this.mutator.deleteColumn(key, columnFamily, columnName);

				removeIndex(fieldNumber, value, objectProvider, mutator);

				return;

			}

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

					throw new NucleusDataStoreException(
							"Embedded objects are unsupported.  Mark the object as persistent and use a serializable class instead");

				}

				Object persisted = context.persistObjectInternal(value,
						objectProvider, fieldNumber, StateManager.PC);

				// TODO add this data to the supercolumn info

				Object objectPk = context.getApiAdapter().getIdForObject(
						persisted);

				mutator.writeColumn(key, columnFamily, mutator.newColumn(
						columnName, getBytes(objectPk)));

				return;
			}

			if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {
				// Collection/Map/Array

				if (fieldMetaData.hasCollection()) {

					List<Object> serializedKeys = new ArrayList<Object>(
							((Collection<?>) value).size());

					Object persisted = null;
					Object objectPk = null;

					for (Object element : (Collection<?>) value) {
						// persist the object
						persisted = context.persistObjectInternal(element,
								objectProvider, fieldNumber, StateManager.PC);

						objectPk = context.getApiAdapter().getIdForObject(
								persisted);

						serializedKeys.add(objectPk);

					}

					mutator.writeColumn(key, columnFamily, mutator.newColumn(
							columnName, getBytes(serializedKeys)));

					return;

				} else if (fieldMetaData.hasMap()) {

					ApiAdapter adapter = context.getApiAdapter();

					Map<?, ?> map = ((Map<?, ?>) value);

					Map<Object, Object> serializedMap = new HashMap<Object, Object>(
							map.size());

					// serialized values to store per item
					Object serializedKey = null;
					Object serializedValue = null;

					// value set by the user in the map
					Object mapValue = null;

					// pointer to what we persisted
					Object persisted = null;

					// get each element and persist it.
					for (Object mapKey : map.keySet()) {

						mapValue = map.get(mapKey);

						// handle the case if our key is a persistent class
						// itself
						if (adapter.isPersistable(mapKey)) {

							persisted = context.persistObjectInternal(mapKey,
									objectProvider, fieldNumber,
									StateManager.PC);

							serializedKey = context.getApiAdapter()
									.getIdForObject(persisted);
						} else {
							serializedKey = mapKey;
						}

						// persist the value if it can be persisted
						if (adapter.isPersistable(mapValue)) {

							persisted = context.persistObjectInternal(mapValue,
									objectProvider, fieldNumber,
									StateManager.PC);

							serializedValue = context.getApiAdapter()
									.getIdForObject(persisted);
							;
						} else {
							serializedKey = mapValue;
						}

						serializedMap.put(serializedKey, serializedValue);

					}

					mutator.writeColumn(key, columnFamily, mutator.newColumn(
							columnName, getBytes(serializedMap)));

					return;

				} else if (fieldMetaData.hasArray()) {

					List<Object> serializedKeys = new ArrayList<Object>(Array
							.getLength(value));

					Object persisted = null;
					Object objectPk = null;

					for (int i = 0; i < Array.getLength(value); i++) {
						// persist the object
						persisted = context.persistObjectInternal(Array.get(
								value, i), objectProvider, fieldNumber,
								StateManager.PC);

						objectPk = context.getApiAdapter().getIdForObject(
								persisted);

						serializedKeys.add(objectPk);
					}

					mutator.writeColumn(key, columnFamily, mutator.newColumn(
							columnName, getBytes(serializedKeys)));

				}

				return;
			}

			// see if we have an objecttoString converter. If we do convert it.

			ObjectStringConverter converter = this.context.getTypeManager()
					.getStringConverter(fieldMetaData.getType());

			if (converter != null) {

				mutator.writeColumn(key, columnFamily, mutator.newColumn(
						columnName, getBytes(converter.toString(value))));

			} else {
				mutator.writeColumn(key, columnFamily, mutator.newColumn(
						columnName, getBytes(value)));

			}

			indexField(fieldNumber, value, objectProvider, mutator);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {
				mutator.deleteColumn(key, columnFamily, columnName);
				removeIndex(fieldNumber, value, objectProvider, mutator);
				return;
			}
			mutator.writeColumn(key, columnFamily, mutator.newColumn(
					columnName, getBytes(value)));

			indexField(fieldNumber, value, objectProvider, mutator);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	

}
