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

import static com.spidertracks.datanucleus.utils.ByteConverter.getBoolean;
import static com.spidertracks.datanucleus.utils.ByteConverter.getChar;
import static com.spidertracks.datanucleus.utils.ByteConverter.getDouble;
import static com.spidertracks.datanucleus.utils.ByteConverter.getFloat;
import static com.spidertracks.datanucleus.utils.ByteConverter.getInt;
import static com.spidertracks.datanucleus.utils.ByteConverter.getLong;
import static com.spidertracks.datanucleus.utils.ByteConverter.getObject;
import static com.spidertracks.datanucleus.utils.ByteConverter.getShort;
import static com.spidertracks.datanucleus.utils.ByteConverter.getString;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.Relation;
import org.datanucleus.sco.SCOUtils;
import org.datanucleus.store.ExecutionContext;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends CassandraFieldManager {

	private Map<String, Column> columns;
	private AbstractClassMetaData metaData;
	private StateManager stateManager;
	private ExecutionContext context;
	private MetaDataManager metaDataManager;
	private ClassLoaderResolver clr;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraFetchFieldManager(List<Column> columns, StateManager stateManager) {
		super();

		this.stateManager = stateManager;
		this.metaData = stateManager.getClassMetaData();
		this.context = stateManager.getObjectManager().getExecutionContext();
		this.metaDataManager = this.context.getMetaDataManager();
		this.clr = this.context.getClassLoaderResolver();

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<String, Column>();

		for (Column column : columns) {
//			// TODO super columns
//			if (column == null) {
//				continue;
//			}

			this.columns.put(getString(column.name), column);
		}

		
	}

	@Override
	public boolean fetchBooleanField(int fieldNumber) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return false;
			}

			return getBoolean(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public byte fetchByteField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}

			return column.value[0];

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public char fetchCharField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return Character.MIN_VALUE;
			}

			return getChar(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public double fetchDoubleField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}
			return getDouble(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public float fetchFloatField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}
			return getFloat(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public int fetchIntField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}

			return getInt(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public long fetchLongField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}

			return getLong(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object fetchObjectField(int fieldNumber) {
		try {

			
			String columnName = getColumnName(metaData, fieldNumber);
			Column column = columns.get(columnName);
			

			// No object defined
			if (column == null) {
				return null;
			}

			
			AbstractMemberMetaData fieldMetaData = this.metaData.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
			
			int relationType = fieldMetaData.getRelationType(clr);

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {

				// Persistable object
				if (fieldMetaData.isEmbedded()) {
					// TODO Handle embedded objects
					throw new NucleusDataStoreException(
							"Embedded objects are currently unimplemented.");
				}

				String key = getString(column.getValue());
				
				AbstractClassMetaData metaData = this.metaDataManager.getMetaDataForClass(fieldMetaData.getType(), clr);
				
	
				Object object = getObjectFromIdString(key, metaData	);
				

				return stateManager.wrapSCOField(fieldNumber, object, false,	false, true);
				
			
			} else if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {

				if (Collection.class.isAssignableFrom(fieldMetaData.getType())) {
					
					
					
					Collection<Object> coll;
					
					try {
						Class<?> instanceType = SCOUtils.getContainerInstanceType(
								fieldMetaData.getType(), fieldMetaData
										.getOrderMetaData() != null);
						coll = (Collection<Object>) instanceType.newInstance();
					} catch (Exception e) {
						throw new NucleusDataStoreException(e.getMessage(), e);
					}
					
					

					// loop through the super columns
					
					//get our list of Strings
					
					List<String> serializedIdList = getObject(columns.get(columnName).getValue());
					
					AbstractClassMetaData elementCmd = fieldMetaData
					.getCollection().getElementClassMetaData(clr, metaDataManager);
					
					for (String id : serializedIdList) {

						Object element = getObjectFromIdString(id, elementCmd);
						coll.add(element);
					}

					return stateManager.wrapSCOField(fieldNumber, coll, false,
							false, true);
				} else if (Map.class.isAssignableFrom(fieldMetaData.getType())) {

					Map<Object, Object> map;

					try {
						Class<?> instanceType = SCOUtils.getContainerInstanceType(
								fieldMetaData.getType(), fieldMetaData
										.getOrderMetaData() != null);
						map = (Map<Object, Object>) instanceType.newInstance();
					} catch (Exception e) {
						throw new NucleusDataStoreException(e.getMessage(), e);
					}


					ApiAdapter adapter = stateManager.getObjectManager().getExecutionContext().getApiAdapter();

					
					
					Map<Object, Object> serializedMap = getObject(columns.get(columnName).getValue());
					
					Class keyClass = clr.classForName(fieldMetaData.getMap().getKeyType());
					Class valueClass = clr.classForName(fieldMetaData.getMap().getValueType());
					
					AbstractClassMetaData keyCmd = fieldMetaData
					.getMap().getKeyClassMetaData(
							clr,
							metaDataManager);
					
					AbstractClassMetaData valueCmd = fieldMetaData
					.getMap().getValueClassMetaData(
							clr,
							metaDataManager);
		
					
					for(Object mapKey: serializedMap.keySet()) {
						
						Object key = null;

						if (adapter.isPersistable(keyClass)) {
							key = getObjectFromIdString((String)mapKey, keyCmd);

						} else {
							key = mapKey;
						}

						Object mapValue = serializedMap.get(key);
						Object value = null;

						if (adapter.isPersistable(valueClass)) { 
							value = getObjectFromIdString((String)mapValue, valueCmd);
						} else {
							value = mapValue;
						}

						map.put(key, value);
					}
					

					return stateManager.wrapSCOField(fieldNumber, map, false, false, true);

				
				} else if (fieldMetaData.getType().isArray()) {

					
					List<String> keys = getObject(columns.get(columnName).getValue());
					
					Object array = Array.newInstance(fieldMetaData.getType()
							.getComponentType(), keys.size());
					
					AbstractClassMetaData elementCmd = fieldMetaData
					.getArray().getElementClassMetaData(
							clr,
							metaDataManager);

					for (int i = 0; i < keys.size(); i ++) {
						
						Object element = getObjectFromIdString(keys.get(i), elementCmd);

						Array.set(array, Integer.valueOf(i), element);
					}

					return stateManager.wrapSCOField(fieldNumber, array, false,
							false, true);
				}

			}

		
			return getObject(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public short fetchShortField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return 0;
			}

			return getShort(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public String fetchStringField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
				return null;
			}

			// ByteArrayInputStream bis = new
			// ByteArrayInputStream(column.value);
			//			
			//
			// // always return UTF 8 values as UTF 8 shoudl always be stored
			// String value = ois.readUTF();
			//			
			//			

			String value = getString(column.value);

			return value;

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	/**
	 * Convenience method to find an object given a string form of its identity,
	 * and the metadata for the class (or a superclass).
	 * 
	 * @param idStr
	 *            The id string
	 * @param cmd
	 *            Metadata for the class
	 * @return The object
	 */
	protected Object getObjectFromIdString(String idStr,
			AbstractClassMetaData cmd) {


		// convert it to an instance of the type
		Object value = com.spidertracks.datanucleus.utils.MetaDataUtils.getKeyValue(this.stateManager, cmd, idStr);

		Class<?> cls = clr.classForName(cmd.getFullClassName());

		Object id = stateManager.getObjectManager().newObjectId(cls, value);

		//TODO TN, are we sure we don't want to validate?  Doing so causes a recursive load issue
		return stateManager.getObjectManager().findObject(id, false, false, cmd.getFullClassName());

	}
	
}
