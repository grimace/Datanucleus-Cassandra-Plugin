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
package com.spidertracks.datanucleus.query.runtime;

import java.util.Set;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Bytes;

/**
 * Class that represents an || or && operation.  Each will have a left and a right.  This is used to
 * Thread && queries to allow for more efficient unions of results sets with && and || ops
 * @author Todd Nine
 *
 */
public abstract class Operand {
	
	protected Operand parent;
	
	protected Operand left;
	
	protected Operand right;
	
	protected Set<Bytes> candidateKeys;
	
	
	
	
	/**
	 * Called by the child when it has completed it's operation to signal to the parent it is done
	 * @param child
	 */
	public abstract void complete(Operand child);
	
	/**
	 * Will run the query.  
	 */
	public abstract void performQuery(String poolName, String cfName, String identityColumnName, ConsistencyLevel consistency);


	public Set<Bytes> getCandidateKeys() {
		return candidateKeys;
	}


	public void setParent(Operand parent) {
		this.parent = parent;
	}


	public void setLeft(Operand left) {
		this.left = left;
	}


	public void setRight(Operand right) {
		this.right = right;
	}
	
	

}
