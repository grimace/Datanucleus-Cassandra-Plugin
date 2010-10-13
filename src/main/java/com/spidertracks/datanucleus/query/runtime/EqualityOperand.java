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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.datanucleus.exceptions.NucleusException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

/**
 * @author Todd Nine
 *
 */
public class EqualityOperand extends Operand implements CompressableOperand{

	private IndexClause clause;

	
	public EqualityOperand(int count){
		clause = new IndexClause();
		clause.setStart_key(new byte[]{});
		clause.setCount(count);
		super.candidateKeys = new HashSet<Bytes>();
	}
	
	
	/* (non-Javadoc)
	 * @see com.spidertracks.datanucleus.query.runtime.Operand#complete(com.spidertracks.datanucleus.query.runtime.Operand)
	 */
	@Override
	public void complete(Operand child) {
		throw new UnsupportedOperationException("Equality operands should have no children");
	}

	/**
	 * Add the index expression to the clause
	 * @param expression
	 */
	public void addExpression(IndexExpression expression) {
		clause.addToExpressions(expression);
	}
	
	/**
	 * Add all expression to the index clause
	 * @param expressions
	 */
	public void addAll(List<IndexExpression> expressions){
		for(IndexExpression expr: expressions){
			clause.addToExpressions(expr);
		}
	}

	@Override
	public IndexClause getIndexClause() {
		return clause;
	}



	@Override
	public void performQuery(String poolName, String cfName, String identityColumnName,	ConsistencyLevel consistency) {
		
		
		try {
			Map<Bytes, List<Column>> results = Pelops.createSelector(poolName).getIndexedColumns(cfName, clause, Selector.newColumnsPredicate(identityColumnName), consistency);
			
			
			for(List<Column> columns: results.values()){
				
				if(columns == null || columns.size() == 0){
					continue;
				}
				
				super.candidateKeys.add(new Bytes(columns.get(0).value));
			}
			
		} catch (Exception e) {
			throw new NucleusException("Error processing secondary index", e);
		}
		
		//signal to the parent node the query completed
		if(parent != null){
			parent.complete(this);
		}
		
	}
	
	


}
