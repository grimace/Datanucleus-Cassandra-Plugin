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

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getDiscriminatorColumnName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.identity.SingleFieldIdentity;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.scale7.cassandra.pelops.Bytes;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.query.runtime.Columns;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.utils.ByteConverter;
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

		Expression filter = this.getCompilation().getExprFilter();

		boolean evaluteInMemory = true;

		String poolName = ((CassandraStoreManager) ec.getStoreManager())
				.getPoolName();

		AbstractClassMetaData acmd = ec.getMetaDataManager()
				.getMetaDataForClass(candidateClass.getName(),
						ec.getClassLoaderResolver());

		String columnFamily = MetaDataUtils.getColumnFamily(acmd);

		String identityCol = MetaDataUtils.getIdentityColumn(acmd);

		if (filter == null) {
			throw new NucleusDataStoreException(
					"You cannot invoke a query without an expression against Cassandra. In a real environment the result set would be too large to process");
		}

		ClassLoaderResolver clr = ec.getClassLoaderResolver();

		String descriminiatorCol = null;
		DiscriminatorMetaData discriminator = null;

		String[] selectColumns = new String[] { identityCol };

		CassandraQueryExpressionEvaluator evaluator = new CassandraQueryExpressionEvaluator(
				ec, parameters, clr, candidateClass);

		Operand opTree = (Operand) filter.evaluate(evaluator);

		evaluteInMemory = evaluator.isInMemoryRequired();

		Bytes idColumnBytes = Bytes.fromUTF8(identityCol);
		Bytes descriminatorBytes = null;

		if (acmd.hasDiscriminatorStrategy()) {
			discriminator = acmd.getDiscriminatorMetaData();

			descriminiatorCol = getDiscriminatorColumnName(discriminator);

			descriminatorBytes = Bytes.fromUTF8(descriminiatorCol);

			List<String> descriminatorValues = MetaDataUtils
					.getDescriminatorValues(acmd.getFullClassName(), clr, ec);

			opTree = opTree.optimizeDescriminator(descriminiatorCol, descriminatorValues);

			selectColumns = new String[] { identityCol, descriminiatorCol };
		}

		// perform a query rewrite to take into account descriminator values

		// TODO, reconstruct query if there is a descriminator
		opTree.performQuery(poolName, columnFamily, selectColumns,
				ConsistencyLevel.ONE);

		Set<Columns> candidateKeys = opTree.getCandidateKeys();

		Collection<?> results = getObjectsOfCandidateType(candidateKeys, acmd,
				clr, subclasses, idColumnBytes, descriminatorBytes);

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

	/**
	 * Used to load specific keys
	 * 
	 * @param ec
	 * @param candidateClass
	 * @param keys
	 * @param subclasses
	 * @param ignoreCache
	 * @param limit
	 * @param startKey
	 * @return
	 */
	public List<?> getObjectsOfCandidateType(Set<Columns> keys,
			AbstractClassMetaData acmd, ClassLoaderResolver clr,
			boolean subclasses, Bytes identityColumn,
			Bytes descriminatorColumn) {

		// final ClassLoaderResolver clr = ec.getClassLoaderResolver();
		// final AbstractClassMetaData acmd =
		// ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);

		List<Object> results = new ArrayList<Object>(keys.size());
		// String tempKey = null;

		for (Columns idBytes : keys) {

			Class<?> targetClass = candidateClass;

			if (descriminatorColumn != null) {

				String descriminatorValue = idBytes.getColumnValue(descriminatorColumn).toUTF8();

				String className = org.datanucleus.metadata.MetaDataUtils
						.getClassNameFromDiscriminatorValue(descriminatorValue,
								acmd.getDiscriminatorMetaData(), ec);

				targetClass = clr.classForName(className);

			}

			Object identity = MetaDataUtils.getObjectIdentity(ec, targetClass,
					idBytes.getColumnValue(identityColumn).getBytes());

			// Not a valid subclass, don't return it as a candidate
			if (!(identity instanceof SingleFieldIdentity)) {
				throw new NucleusDataStoreException(
						"Only single field identities are supported");
			}

			if (!ClassUtils.typesAreCompatible(candidateClass,
					((SingleFieldIdentity) identity).getTargetClassName(), clr)) {
				continue;
			}

			Object returned = ec.findObject(identity, true, subclasses,
					candidateClass.getName());

			if (returned != null) {
				results.add(returned);
			}
		}

		return results;

	}
}
