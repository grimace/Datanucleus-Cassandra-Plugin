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

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.service.BatchMutation;

import org.datanucleus.StateManager;
import org.datanucleus.store.ExecutionContext;

/**
 * Internalizes all pending operations for a given Execution context.
 * 
 * @author Todd Nine
 * 
 */
public class BatchMutationManager {

	private Map<ExecutionContext, ExecutionContextMutate> contextMutations = new HashMap<ExecutionContext, ExecutionContextMutate>();

	public BatchMutationManager() {

	}

	public void beginWrite(ExecutionContext context, StateManager sm) {
		getMutations(context).pushInstance();
	}

	/**
	 * Returns true if this is the last object to end writing. This mean the
	 * mutation for this context should be saved
	 * 
	 * @param context
	 * @param sm
	 * @return
	 */
	public BatchMutation endWrite(ExecutionContext context, StateManager sm) {
		// not our root instance, don't create a batch mutation
		if (!getMutations(context).popInstance()) {
			return null;
		}

		// it is our root instance, create the batch mutation.

		BatchMutation changes = getMutations(context).createBatchMutation();
		removeMutations(context);

		return changes;

	}

	/**
	 *  Add the delete to the current execution context operations
	 * @param context
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param timestamp
	 */
	public void addDelete(ExecutionContext context, String columnFamily,
			String rowKey, String columnName, long timestamp) {
		getMutations(context).addDelete(columnFamily, rowKey, columnName,
				timestamp);
	}

	/**
	 * Add the column with data to the current execution context operations
	 * @param context
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param value
	 * @param timestamp
	 */
	public void addColumn(ExecutionContext context, String columnFamily,
			String rowKey, String columnName, byte[] value, long timestamp) {
		getMutations(context).addColumn(columnFamily, rowKey, columnName,
				value, timestamp);
	}

	/**
	 * Add the super column with data to the current execution context operations Not currently used
	 * @param context
	 * @param columnFamily
	 * @param rowKey
	 * @param superColumnName
	 * @param columnName
	 * @param value
	 * @param timestamp
	 */
//	public void addSuperColumn(ExecutionContext context, String columnFamily,
//			String rowKey, String superColumnName, String columnName,
//			byte[] value, long timestamp) {
//		getMutations(context).addSuperColumn(columnFamily, rowKey,
//				superColumnName, columnName, value, timestamp);
//	}

	/**
	 * Get the mutations for this execution context
	 * 
	 * @param context
	 * @return
	 */
	private ExecutionContextMutate getMutations(ExecutionContext context) {
		ExecutionContextMutate operations = contextMutations.get(context);

		if (operations == null) {
			operations = new ExecutionContextMutate(context);
			contextMutations.put(context, operations);
		}

		return operations;
	}

	/**
	 * Remove our mutations from the cache
	 * 
	 * @param context
	 */
	private void removeMutations(ExecutionContext context) {
		contextMutations.remove(context);
	}

}
