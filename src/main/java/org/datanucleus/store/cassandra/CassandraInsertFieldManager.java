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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
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

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(List<Column> updates, List<Deletion> deletes, AbstractClassMetaData metaData) {
		super();

		this.metaData = metaData;

		this.updates = updates;
		this.deletes = deletes;

	}
	
	
	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {

			String columnName = getColumnName(metaData, fieldNumber);
			
			
			ByteArrayOutputStream bis = new ByteArrayOutputStream();
			ObjectOutputStream ois = new ObjectOutputStream(bis);

//			
//			(columnName, ois.writeBoolean(value));
			ois.close();
			bis.close();
			
			Mutation mut = new Mutation();
			mut.column_or_supercolumn();

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {
		// TODO Auto-generated method stub
		super.storeByteField(fieldNumber, value);
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {
		// TODO Auto-generated method stub
		super.storeCharField(fieldNumber, value);
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {
		// TODO Auto-generated method stub
		super.storeDoubleField(fieldNumber, value);
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {
		// TODO Auto-generated method stub
		super.storeFloatField(fieldNumber, value);
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {
		// TODO Auto-generated method stub
		super.storeIntField(fieldNumber, value);
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {
		// TODO Auto-generated method stub
		super.storeLongField(fieldNumber, value);
	}
	

	@Override
	public void storeShortField(int fieldNumber, short value) {
		// TODO Auto-generated method stub
		super.storeShortField(fieldNumber, value);
	}
	
	//these values can be deleted, so we'll want to 

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		// TODO Auto-generated method stub
		super.storeObjectField(fieldNumber, value);
	}


	@Override
	public void storeStringField(int fieldNumber, String value) {
		// TODO Auto-generated method stub
		super.storeStringField(fieldNumber, value);
	}
	
	protected ColumnPath getColumnPath(AbstractClassMetaData metaData, int absoluteFieldNumber){
		ColumnPath columnPath = new ColumnPath(metaData.getTable());
		columnPath.setColumn(getBytes(getColumnName(metaData, absoluteFieldNumber)));
		
		return columnPath;
	}

}
