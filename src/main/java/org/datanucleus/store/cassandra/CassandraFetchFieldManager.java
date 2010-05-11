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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends CassandraFieldManager {

	private Map<String, Column> columns;
	private AbstractClassMetaData metaData;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraFetchFieldManager(List<Column> columns,
			AbstractClassMetaData metaData) {
		super();

		this.metaData = metaData;

		// rather than iterate over every field call for O(n) it's faster to
		// take our O(n) hit up front then perform an O(1) lookup. Sorting and
		// searching is O(n log (n)) sort plus log n search
		this.columns = new HashMap<String, Column>();

		for (Column column : columns) {
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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);

			boolean value = ois.readBoolean();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);

			byte value = ois.readByte();
			ois.close();
			bis.close();
			return value;

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
				return '\u0000';
			}
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			char value = ois.readChar();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			double value = ois.readDouble();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			float value = ois.readFloat();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			int value = ois.readInt();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			long value = ois.readLong();
			ois.close();
			bis.close();
			return value;

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

			

			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			Object value = ois.readObject();
			ois.close();
			bis.close();
			return value;

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
			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			short value = ois.readShort();
			ois.close();
			bis.close();
			return value;

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

			ByteArrayInputStream bis = new ByteArrayInputStream(column.value);
			ObjectInputStream ois = new ObjectInputStream(bis);

			// always return UTF 8 values as UTF 8 shoudl always be stored
			String value = ois.readUTF();
			ois.close();
			bis.close();
			return value;

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

}
