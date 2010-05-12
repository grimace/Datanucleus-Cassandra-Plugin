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
package org.datanucleus.store.cassandra.mutate;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import me.prettyprint.cassandra.service.BatchMutation;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SuperColumn;
import org.datanucleus.store.ExecutionContext;

/**
 * Holds all mutations for the current execution context
 * 
 * @author Todd Nine
 * 
 */
public class ExecutionContextMutate {

	// our map by object class and column family
	private Map<Key, ColumnFamilyMutate> mutations = new HashMap<Key, ColumnFamilyMutate>();

	private ExecutionContext ctx;
	private Stack<Object> instances;

	public ExecutionContextMutate(ExecutionContext ctx) {
		this.ctx = ctx;
		this.instances = new Stack<Object>();
	}

	/**
	 * Get the execution context this mutate belongs to
	 * 
	 * @return
	 */
	public ExecutionContext getExecutionContext() {
		return ctx;
	}

	/**
	 * Add the delete to the current execution context operations
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param timestamp
	 */
	public void AddDelete(String columnFamily, String rowKey,
			String columnName, long timestamp) {
		getMutation(columnFamily, rowKey).AddDelete(columnName, timestamp);
	}

	/**
	 * Add the column with data to the current execution context operations
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param value
	 * @param timestamp
	 */
	public void AddColumn(String columnFamily, String rowKey,
			String columnName, byte[] value, long timestamp) {
		getMutation(columnFamily, rowKey).AddColumn(columnName, value,
				timestamp);
	}

	/**
	 * Add the super column with data to the current execution context
	 * operations
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @param superColumnName
	 * @param columnName
	 * @param value
	 * @param timestamp
	 */
	public void AddSuperColumn(String columnFamily, String rowKey,
			String superColumnName, String columnName, byte[] value,
			long timestamp) {
		getMutation(columnFamily, rowKey).AddSuperColumn(superColumnName,
				columnName, value, timestamp);
	}

	/**
	 * Push the current on to our stack for this execution context
	 * 
	 * @param instance
	 */
	public void pushInstance() {
		instances.push(new Object());
	}

	/**
	 * Pop the current instance from our execution context
	 * 
	 * @param instnace
	 * @return
	 */
	public boolean popInstance() {
		instances.pop();

		return instances.size() == 0;

	}

	/**
	 * Get our mutation manager for this column family. If one doesn't exist
	 * it's created
	 * 
	 * @param columnFamily
	 * @param rowKey
	 * @return
	 */
	private ColumnFamilyMutate getMutation(String columnFamily, String rowKey) {

		Key key = new Key(columnFamily, rowKey);

		ColumnFamilyMutate family = mutations.get(key);

		if (family == null) {

			family = new ColumnFamilyMutate(columnFamily, rowKey);

			mutations.put(key, family);
		}

		return family;

	}

	public BatchMutation createBatchMutation() {

		BatchMutation changes = new BatchMutation();

		for (ColumnFamilyMutate columnFamily : mutations.values()) {

			// tell cassandra which family to perform the op on
			Stack<String> columnFamilies = new Stack<String>();
			columnFamilies.add(columnFamily.getColumnFamily());

			String key = columnFamily.getKey();

			// now build all the ops from the current context.

		
			for (Column column : columnFamily.getColumns()) {
				changes.addInsertion(key, columnFamilies, column);
			}

			for (SuperColumn superColumn : columnFamily.getSuperColumns()) {
				changes.addSuperInsertion(key, columnFamilies, superColumn);
			}

			for (Deletion deletion : columnFamily.getDeletes()) {
				changes.addDeletion(key, columnFamilies, deletion);
			}

		}

		return changes;

	}

	private class Key {

		private String key;
		private String columnFamily;

		public Key(String columnFamily, String key) {
			this.key = key;
			this.columnFamily = columnFamily;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result
					+ ((columnFamily == null) ? 0 : columnFamily.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (columnFamily == null) {
				if (other.columnFamily != null)
					return false;
			} else if (!columnFamily.equals(other.columnFamily))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}

		private ExecutionContextMutate getOuterType() {
			return ExecutionContextMutate.this;
		}

	}

}
