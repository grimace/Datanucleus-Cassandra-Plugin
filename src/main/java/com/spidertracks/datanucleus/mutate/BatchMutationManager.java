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

Contributors : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus.mutate;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.datanucleus.store.ExecutionContext;
import org.wyki.cassandra.pelops.Mutator;

/**
 * Internalises all pending operations for a given Execution context.
 * 
 * @author Todd Nine
 * 
 */
public class BatchMutationManager {

	private Map<ExecutionContext, ExecutionContextMutate> contextMutations = new HashMap<ExecutionContext, ExecutionContextMutate>();

	public BatchMutationManager() {

	}

	public void beginWrite(ExecutionContext context, Mutator mutator) {
		ExecutionContextMutate mutationContext = getMutations(context);
		mutationContext.pushInstance();
		mutationContext.pushMutation(mutator);
	}

	/**
	 * Returns true if this is the last object to end writing. This mean the
	 * mutation for this context should be saved
	 * 
	 * @param context
	 * @param sm
	 * @return
	 * @throws Exception 
	 */
	public void endWrite(ExecutionContext context, ConsistencyLevel consistency) throws Exception {
		// not our root instance, don't create a batch mutation
		if (!getMutations(context).popInstance()) {
			return;
		}

		// it is our root instance, create the batch mutation.

		getMutations(context).execute(consistency);
		removeMutations(context);


	}
	
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
