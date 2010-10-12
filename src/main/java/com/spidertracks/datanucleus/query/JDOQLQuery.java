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
package com.spidertracks.datanucleus.query;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.NucleusLogger;
import org.scale7.cassandra.pelops.Bytes;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * @author Todd Nine
 * 
 */
public class JDOQLQuery extends AbstractJDOQLQuery {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 private static final long serialVersionUID = 1L;
	 * 
	 * /** Constructs a new query instance that uses the given persistence
	 * manager.
	 * 
	 * @param om
	 *            the associated ExecutiongContext for this query.
	 */
	public JDOQLQuery(ExecutionContext ec) {
		this(ec, (JDOQLQuery) null);
	}

	/**
	 * Constructs a new query instance having the same criteria as the given
	 * query.
	 * 
	 * @param om
	 *            The Executing Manager
	 * @param q
	 *            The query from which to copy criteria.
	 */
	public JDOQLQuery(ExecutionContext ec, JDOQLQuery q) {
		super(ec, q);
	}

	/**
	 * Constructor for a JDOQL query where the query is specified using the
	 * "Single-String" format.
	 * 
	 * @param ec
	 *            The execution context
	 * @param query
	 *            The query string
	 */
	public JDOQLQuery(ExecutionContext ec, String query) {
		super(ec, query);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object performExecute(Map parameters) {

		long startTime = System.currentTimeMillis();

		if (NucleusLogger.QUERY.isDebugEnabled()) {
			NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL",
					getSingleStringQuery(), null));
		}

		Collection results = null;

		Expression filter = this.getCompilation().getExprFilter();

		boolean evaluteInMemory = true;
		
		CassandraQuery query = new CassandraQuery(ec, candidateClass);
		
		
		String poolName = ((CassandraStoreManager)ec.getStoreManager()).getPoolName();
		
		
		AbstractClassMetaData acmd =  ec.getMetaDataManager().getMetaDataForClass(candidateClass.getName(), ec.getClassLoaderResolver());
		
		String columnFamily = MetaDataUtils.getColumnFamily(acmd);
		
		String identityCol = MetaDataUtils.getIdentityColumn(acmd);
		
		

		if (filter != null) {

			CassandraQueryExpressionEvaluator evaluator = new CassandraQueryExpressionEvaluator(
					ec, parameters, ec.getClassLoaderResolver(), candidateClass);

			Operand opTree = (Operand) filter.evaluate(evaluator);

			evaluteInMemory = evaluator.isInMemoryRequired();
			
			opTree.performQuery(poolName, columnFamily, identityCol, ConsistencyLevel.ONE);
			
			Set<Bytes> candidateKeys = opTree.getCandidateKeys();

			// didn't get any candidates. Note that this means we couldn't
			// evaluate the expression
			// from secondary keys. NOT that the result set was empty
			if (candidateKeys == null) {
				results = query.getObjectsOfCandidateType(subclasses, 1000);
			} else {
				results = query.getObjectsOfCandidateType(candidateKeys, subclasses);
			}

		}
		// there's nothing to filter so get our scan range if required
		else {

			results = query.getObjectsOfCandidateType(subclasses, 1000);
		}

		if (evaluteInMemory) {

			// Apply any result restrictions to the results
			JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, results,
					compilation, parameters, ec.getClassLoaderResolver());

			results = resultMapper.execute(true, true, true, true, true);

		}

		if (NucleusLogger.QUERY.isDebugEnabled()) {
			NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", ""
					+ (System.currentTimeMillis() - startTime)));
		}

		return results;

	}

}
