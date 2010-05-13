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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends CassandraFieldManager {

	private Map<String, Column> columns;
	private Map<String, SuperColumn> superColumns;
	private AbstractClassMetaData metaData;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraFetchFieldManager(List<Column> columns, List<SuperColumn> superColumns,	AbstractClassMetaData metaData) {
		super();

		this.metaData = metaData;

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<String, Column>();
		this.superColumns = new HashMap<String, SuperColumn>();

		for (Column column : columns) {
			//TODO super columns are returned as nulls.  This may be a bug
			if(column == null){
				continue;
			}
			
			this.columns.put(getString(column.name), column);
		}
		

		for (SuperColumn column : superColumns) {
			this.superColumns.put(getString(column.name), column);
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
			Column column = this.columns.get(columnName);

			// if there's no column defined, this record could be an older
			// record, and this is a new field just return the java default
			// value
			if (column == null) {
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

}
