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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;

/**
 * @author Todd Nine
 * 
 */
public class CassandraInsertFieldManager extends CassandraFieldManager {

	private List<Column> updates;
	private List<Deletion> deletes;
	private AbstractClassMetaData metaData;
	private long updateTimestamp;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(List<Column> updates,
			List<Deletion> deletes, long updateTimestamp,
			AbstractClassMetaData metaData) {
		super();

		this.metaData = metaData;

		this.updates = updates;
		this.deletes = deletes;
		this.updateTimestamp = updateTimestamp;

	}

	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {

			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeBoolean(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeByte(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {

			if(isKey(fieldNumber)){
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeChar(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {

			if(isKey(fieldNumber)){
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeDouble(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {


			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeFloat(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {
			
			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeInt(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {

			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeLong(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {

			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeShort(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	// these values can be deleted, so we'll want to handle deletes if they're
	// null

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		try {
			

			if(isKey(fieldNumber)){
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {

				SlicePredicate slicePredicate = new SlicePredicate();
				slicePredicate.addToColumn_names(getBytes(columnName));
				Deletion delete = new Deletion(this.updateTimestamp);

				delete.setPredicate(slicePredicate);

				this.deletes.add(delete);
				return;

			}

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(value);

			Column column = new Column(getBytes(columnName), bos.toByteArray(),
					this.updateTimestamp);

			oos.close();
			bos.close();

			updates.add(column);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {


			if(isKey(fieldNumber)){
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {

				SlicePredicate slicePredicate = new SlicePredicate();
				slicePredicate.addToColumn_names(getBytes(columnName));
				Deletion delete = new Deletion(this.updateTimestamp);

				delete.setPredicate(slicePredicate);

				this.deletes.add(delete);
				return;

			}

			Column column = new Column(getBytes(columnName), getBytes(value),
					this.updateTimestamp);

			updates.add(column);

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
	
	/**
	 * Returns true if the field is a key
	 * @param fieldNumber
	 * @return
	 */
	protected boolean isKey(int fieldNumber){

		return metaData.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).isPrimaryKey();
		
		
	}

}
