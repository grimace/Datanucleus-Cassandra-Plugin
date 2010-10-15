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
import org.datanucleus.store.types.ObjectLongConverter;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.sco.SCOUtils;
import org.scale7.cassandra.pelops.Bytes;

import com.spidertracks.datanucleus.serialization.Serializer;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends AbstractFieldManager {

	private Serializer serializer;
	private Map<String, Column> columns;
	private AbstractClassMetaData metaData;
	private ObjectProvider objectProvider;
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
		this.serializer = ((CassandraStoreManager)context.getStoreManager()).getSerializer();

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<String, Column>();

		for (Column column : columns) {
			this.columns.put(Bytes.toUTF8(column.getName()), column);
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

			return new Bytes(column.value).toBoolean();

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

			return new Bytes(column.value).toChar();

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
			return new Bytes(column.value).toDouble();

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
			return new Bytes(column.value).toFloat();

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

			return new Bytes(column.value).toInt();

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

			return new Bytes(column.value).toLong();

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

				Object key = serializer.getObject(column.getValue());

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

					List<Object> serializedIdList = serializer.getObject(columns.get(
							columnName).getValue());

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

					Map<Object, Object> serializedMap = serializer.getObject(columns.get(
							columnName).getValue());

					Class keyClass = clr.classForName(fieldMetaData.getMap()
							.getKeyType());
					Class valueClass = clr.classForName(fieldMetaData.getMap()
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

					List<Object> keys = serializer.getObject(columns.get(columnName)
							.getValue());

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

			ObjectLongConverter longConverter = this.context.getTypeManager().getLongConverter(fieldMetaData.getType());
			
			
			if(longConverter != null){
				return longConverter.toObject(new Bytes(column.getValue()).toLong());
			}
			
			ObjectStringConverter converter = this.context.getTypeManager()
					.getStringConverter(fieldMetaData.getType());

			if (converter != null) {
				return converter.toObject(Bytes.toUTF8(column.getValue()));
			}

			return serializer.getObject(column.value);

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

			return new Bytes(column.value).toShort();

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

			return Bytes.toUTF8(column.value);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

}
