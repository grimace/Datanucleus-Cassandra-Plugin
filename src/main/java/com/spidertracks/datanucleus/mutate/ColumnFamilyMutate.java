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
package com.spidertracks.datanucleus.mutate;

import static com.spidertracks.datanucleus.utils.ByteConverter.getBytes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.datanucleus.exceptions.NucleusDataStoreException;

/**
 * @author Todd Nine
 * 
 */
public class ColumnFamilyMutate {

	private String columnFamily;
	
	private String key;

//	private Map<String, SuperColumn> superColumns;

	private  Map<String, Column> columns;

	private  Map<String, Deletion> deletes;
	
	public ColumnFamilyMutate(String columnFamily, String key){
		this.key = key;
		this.columnFamily = columnFamily;
		
//		superColumns = new HashMap<String, SuperColumn>();
		columns =  new HashMap<String, Column>();
		deletes =  new HashMap<String, Deletion>();
	}
	

	public String getKey() {
		return key;
	}

	public String getColumnFamily(){
		return columnFamily;
	}
	
//	public Collection<SuperColumn> getSuperColumns() {
//		return superColumns.values();
//	}

	public Collection<Column> getColumns() {
		return columns.values();
	}

	public Collection<Deletion> getDeletes() {
		return deletes.values();
	}

	
	/**
	 * Add a delete operation
	 * @param columnName
	 * @param timestamp
	 */
	public void addDelete(String columnName, long timestamp ){
		
		if(deletes.containsKey(columnName)){
			throw new NucleusDataStoreException(String.format("You are attempting to perform multiple deletes on column %s in the same instnace", columnName));
		}
		

		SlicePredicate slicePredicate = new SlicePredicate();
		slicePredicate.addToColumn_names(getBytes(columnName));
		Deletion delete = new Deletion(timestamp);

		delete.setPredicate(slicePredicate);
		
		this.deletes.put(columnName, delete);
	}
	
	/**
	 * Deletes the entire record
	 */
	public void addDelete(){
		SlicePredicate slicePredicate = new SlicePredicate();
		slicePredicate.addToColumn_names(getBytes(columnFamily));
		
		Deletion deletion = new Deletion();
		deletion.setPredicate(slicePredicate);
		
		this.deletes.put(columnFamily, deletion);
		
	}

	/**
	 * Add the operation to the column
	 * @param columnName
	 * @param value
	 * @param timestamp
	 */
	public void addColumn(String columnName, byte[] value, long timestamp){

		if(columns.containsKey(columnName)){
			throw new NucleusDataStoreException(String.format("You are attempting to perform multiple sets on column %s in the same instnace", columnName));
			
		}
		
		columns.put(columnName, new Column(getBytes(columnName), value, timestamp));
	}
	
//	/**
//	 * Add data for a supercolumn
//	 * @param superColumnName
//	 * @param columnName
//	 * @param value
//	 * @param timestamp
//	 */
//	public void addSuperColumn(String superColumnName, String columnName, byte[] value, long timestamp){
//		
//		SuperColumn superColumn = superColumns.get(superColumnName);
//		
//		if(superColumn == null){
//			superColumn = new SuperColumn();
//			superColumn.name = getBytes(superColumnName);
//			
//			superColumns.put(superColumnName, superColumn);
//		}
//		
//		superColumn.addToColumns(new Column(getBytes(columnName), value, timestamp));
//		
//	}
//	
	

}
