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

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnName;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.types.sco.SCOUtils;
import org.scale7.cassandra.pelops.Bytes;

import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends AbstractFieldManager {

	private Map<Bytes, Bytes> columns;
	private AbstractClassMetaData metaData;
	private ObjectProvider objectProvider;
	private ByteConverterContext byteContext;
	private ExecutionContext context;
	private ClassLoaderResolver clr;
	
	

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraFetchFieldManager(List<Column> columns, ObjectProvider op) {
		super();

		this.objectProvider = op;
		this.metaData = op.getClassMetaData();
		this.context = op.getExecutionContext();
		this.clr = this.context.getClassLoaderResolver();
		this.byteContext =  ((CassandraStoreManager)context.getStoreManager()).getByteConverterContext();
		

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<Bytes, Bytes>();

		for (Column column : columns) {
			this.columns.put(Bytes.fromBytes(column.getName()), Bytes.fromBytes(column.getValue()));
		}

		

	}

	@Override
	public boolean fetchBooleanField(int fieldNumber) {

		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes value = this.columns.get(columnName);

		

			return (Boolean) byteContext.getBoolean(value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public byte fetchByteField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes value = this.columns.get(columnName);

	

			return value.toByte();

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public char fetchCharField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes value = this.columns.get(columnName);


			return (Character) byteContext.getCharacter(value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public double fetchDoubleField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes value = this.columns.get(columnName);

		
			return (Double) byteContext.getDouble(value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public float fetchFloatField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes value = this.columns.get(columnName);

			return (Float) byteContext.getFloat(value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public int fetchIntField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes column = this.columns.get(columnName);

	
			return (Integer) byteContext.getInteger(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public long fetchLongField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes column = this.columns.get(columnName);

			return (Long) byteContext.getLong(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object fetchObjectField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes column = columns.get(columnName);

			// No object defined
			if (column == null) {
				return null;
			}

			AbstractMemberMetaData fieldMetaData = this.metaData
					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

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

				Serializable key = (Serializable) byteContext.convertToObject(column, this.context, Serializable.class);

				Object object = context.findObject(key, false, false,
						fieldMetaData.getTypeName());

				return objectProvider.wrapSCOField(fieldNumber, object, false,
						false, true);

			} else if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {

				if (Collection.class.isAssignableFrom(fieldMetaData.getType())) {

					Collection<Object> coll;

					try {
						Class<?> instanceType = SCOUtils
								.getContainerInstanceType(fieldMetaData
										.getType(), fieldMetaData
										.getOrderMetaData() != null);
						coll = (Collection<Object>) instanceType.newInstance();
					} catch (Exception e) {
						throw new NucleusDataStoreException(e.getMessage(), e);
					}

					// loop through the super columns

					// get our list of Strings

					List<Object> serializedIdList = (List<Object>) byteContext.convertToObject(column, context, this.metaData, fieldNumber);

					for (Object key : serializedIdList) {

						Object element = context.findObject(key, false, false,
								fieldMetaData.getTypeName());
						coll.add(element);
					}

					return objectProvider.wrapSCOField(fieldNumber, coll,
							false, false, true);
				} else if (Map.class.isAssignableFrom(fieldMetaData.getType())) {

					Map<Object, Object> map;

					try {
						Class<?> instanceType = SCOUtils
								.getContainerInstanceType(fieldMetaData
										.getType(), fieldMetaData
										.getOrderMetaData() != null);
						map = (Map<Object, Object>) instanceType.newInstance();
					} catch (Exception e) {
						throw new NucleusDataStoreException(e.getMessage(), e);
					}

					ApiAdapter adapter = objectProvider.getExecutionContext()
							.getApiAdapter();

					Map<Object, Object> serializedMap = (Map<Object, Object>) byteContext.convertToObject(column, context, Map.class);

					Class<?> keyClass = clr.classForName(fieldMetaData.getMap()
							.getKeyType());
					Class<?> valueClass = clr.classForName(fieldMetaData.getMap()
							.getValueType());

					for (Object mapKey : serializedMap.keySet()) {

						Object key = null;

						if (adapter.isPersistable(keyClass)) {
							key = context.findObject(mapKey, false, false,
									fieldMetaData.getTypeName());
						} else {
							key = mapKey;
						}

						Object mapValue = serializedMap.get(key);
						Object value = null;

						if (adapter.isPersistable(valueClass)) {
							value = context.findObject(mapValue, false, false,
									fieldMetaData.getTypeName());
						} else {
							value = mapValue;
						}

						map.put(key, value);
					}

					return objectProvider.wrapSCOField(fieldNumber, map, false,
							false, true);

				} else if (fieldMetaData.getType().isArray()) {

					List<Object> keys = (List<Object>) byteContext.convertToObject(column, context, List.class);

					Object array = Array.newInstance(fieldMetaData.getType()
							.getComponentType(), keys.size());

					for (int i = 0; i < keys.size(); i++) {

						Object element = context.findObject(keys.get(i), false,
								false, fieldMetaData.getTypeName());

						Array.set(array, Integer.valueOf(i), element);
					}

					return objectProvider.wrapSCOField(fieldNumber, array,
							false, false, true);
				}

			}

			return byteContext.convertToObject(column, context, this.metaData, fieldNumber);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public short fetchShortField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes column = this.columns.get(columnName);

			return (Short) byteContext.getShort(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public String fetchStringField(int fieldNumber) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);
			Bytes column = this.columns.get(columnName);

			return (String) byteContext.getString(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

}
