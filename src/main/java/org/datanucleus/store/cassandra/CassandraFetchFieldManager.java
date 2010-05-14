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

import static org.datanucleus.store.cassandra.utils.ByteConverter.getBoolean;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getChar;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getDouble;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getFloat;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getInt;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getLong;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getObject;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getShort;
import static org.datanucleus.store.cassandra.utils.ByteConverter.getString;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.Relation;
import org.datanucleus.sco.SCOUtils;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends CassandraFieldManager {

	private Map<String, Column> columns;
	private Map<String, SuperColumn> superColumns;
	private AbstractClassMetaData metaData;
	private StateManager stateManager;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraFetchFieldManager(List<Column> columns,
			List<SuperColumn> superColumns, StateManager stateManager) {
		super();

		this.stateManager = stateManager;
		this.metaData = stateManager.getClassMetaData();

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<String, Column>();

		for (Column column : columns) {
			// TODO super columns are returned as nulls. This may be a bug
			if (column == null) {
				continue;
			}

			this.columns.put(getString(column.name), column);
		}

		if (superColumns != null) {
			this.superColumns = new HashMap<String, SuperColumn>();

			for (SuperColumn column : superColumns) {
				this.superColumns.put(getString(column.name), column);
			}
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

	@Override
	public Object fetchObjectField(int fieldNumber) {
		try {

			String columnName = getColumnName(metaData, fieldNumber);
		

			ExecutionContext ec = stateManager.getObjectProvider()
					.getExecutionContext();
			ClassLoaderResolver clr = ec.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = stateManager
					.getClassMetaData()
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

				// // Stored as a String reference, so retrieve the string form
				// of the identity, and find the object
				// String idStr = cell.getRichStringCellValue().getString();
				//		            
				//		            
				// if (idStr == null)
				// {
				// return null;
				// }
				//
				// if (idStr.startsWith("[") && idStr.endsWith("]"))
				// {
				// idStr = idStr.substring(1, idStr.length()-1);
				// AbstractClassMetaData relatedCmd =
				// ec.getMetaDataManager().getMetaDataForClass(fieldMetaData.getType(),
				// clr);
				// return getObjectFromIdString(idStr, relatedCmd);
				// }
				// else
				// {
				// return null;
				// }

			} else if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {

				if (Collection.class.isAssignableFrom(fieldMetaData.getType())) {
					Collection<Object> coll;
					try {
						Class instanceType = SCOUtils.getContainerInstanceType(
								fieldMetaData.getType(), fieldMetaData
										.getOrderMetaData() != null);
						coll = (Collection<Object>) instanceType.newInstance();
					} catch (Exception e) {
						throw new NucleusDataStoreException(e.getMessage(), e);
					}

					// loop through the super columns

					SuperColumn collection = this.superColumns.get(columnName);

					for (Column col : collection.columns) {
//
						AbstractClassMetaData elementCmd = fieldMetaData
								.getCollection().getElementClassMetaData(
										ec.getClassLoaderResolver(),
										ec.getMetaDataManager());
						

//						AbstractClassMetaData elementCmd = fieldMetaData.getCollection().
								
						Object element = getObjectFromIdString(
								getString(col.value), elementCmd);
						coll.add(element);
					}

					return stateManager.wrapSCOField(fieldNumber, coll, false,
							false, true);
				} else if (Map.class.isAssignableFrom(fieldMetaData.getType())) {
					throw new NucleusException(
							"maps are currently unimplemented.");
				} else if (fieldMetaData.getType().isArray()) {
					throw new NucleusException(
							"arrays are currently unimplemented.");
				}

			}
			
			Column column = this.columns.get(columnName);
			
			//No object defined
			if(column == null){
				return null;
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
		ExecutionContext ec = stateManager.getObjectProvider()
				.getExecutionContext();
		ClassLoaderResolver clr = ec.getClassLoaderResolver();
		Object id = null;
		if (cmd.usesSingleFieldIdentityClass()) {
			id = ec.getApiAdapter().getNewApplicationIdentityObjectId(clr, cmd,
					idStr);
		} else {
			Class cls = clr.classForName(cmd.getFullClassName());
			id = stateManager.getObjectManager().newObjectId(cls, idStr);
		}
		return stateManager.getObjectManager().findObject(id, true, true, null);
	}

}
