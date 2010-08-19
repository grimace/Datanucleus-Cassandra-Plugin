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

import static com.spidertracks.datanucleus.utils.ByteConverter.getObject;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.CaseExpression;
import org.datanucleus.query.expression.CreatorExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.SubqueryExpression;
import org.datanucleus.query.expression.VariableExpression;
import org.datanucleus.store.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;
import org.wyki.cassandra.pelops.Selector.OrderType;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.utils.ByteConverter;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * Class that will recursively query and merge results from our tree as we're
 * visited. Supports basic result set building from secondary indexes. Will work
 * with < > == && || and limits. Everything else comes from the in memory
 * evaluator because it can't be evaluated with result sets from cassandra
 * 
 * @author Todd Nine
 * 
 */
public class CassandraQueryExpressionEvaluator extends
		AbstractExpressionEvaluator {

	// TODO TN get this from the query
	private static final int MAXCOUNT = 10000;

	private static final Logger logger = LoggerFactory
			.getLogger(CassandraQueryExpressionEvaluator.class);

	private Stack<String> indexNames = new Stack<String>();
	private Stack<IndexParam> indexKeys = new Stack<IndexParam>();
	private Stack<Set<Object>> columnStack = new Stack<Set<Object>>();

	private AbstractClassMetaData metaData;

	// Flag that marks an operation we can't support has been performed. We need
	// to then run result set
	// through the memory scanner
	private boolean inMemoryRequired;

	/** Map of input parameter values, keyed by their name. */
	private Map<String, Object> parameterValues;

	private ExecutionContext ec;

	private Selector selector;
	
	private SlicePredicate idSelector;

	/**
	 * Constructor for an in-memory evaluator.
	 * 
	 * @param ec
	 *            ExecutionContext
	 * @param params
	 *            Input parameters
	 * @param state
	 *            Map of state values keyed by their symbolic name
	 * @param imports
	 *            Any imports
	 * @param clr
	 *            ClassLoader resolver
	 * @param candidateAlias
	 *            Alias for the candidate class. With JDOQL this is "this".
	 */
	public CassandraQueryExpressionEvaluator(ExecutionContext ec,
			Map<String, Object> params, ClassLoaderResolver clr,
			Class<?> destinationClass) {
		this.ec = ec;
		metaData = ec.getMetaDataManager().getMetaDataForClass(
				destinationClass, clr);
		this.parameterValues = (params != null ? params
				: new HashMap<String, Object>());
		// this.state = state;
		// this.imports = imports;

		CassandraStoreManager manager = (CassandraStoreManager) ec
				.getStoreManager();

		selector = Pelops.createSelector(manager.getPoolName(), manager
				.getKeyspace());

		inMemoryRequired = false;
		
		
		int[] identityFields = metaData.getPKMemberPositions();

		// assume only one identity field
	
		idSelector = Selector.newColumnsPredicate(getColumnName(metaData, identityFields[0]));

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processAndExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processAndExpression(Expression expr) {
		logger.debug("Processing && expression {}", expr);

		// if we get here, the right and the left have been evaluated. Pop the
		// left set list x2 and the right set
		// then intersect them

		Set<Object> right = this.columnStack.pop();
		Set<Object> left = this.columnStack.pop();

		Set<Object> result = null;

		// if our left is null set the result to our right
		if (left == null) {
			result = right;
		}
		// if our left isn't null but our right is, set the result to the left
		else if (right == null) {
			result = left;
		}
		// if neither is null, perform an intersection
		else {
			// perform an intersection of the sets
			left.retainAll(right);
			result = left;
		}

		// push the result back on the stack
		this.columnStack.push(result);

		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processEqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processEqExpression(Expression expr) {
		logger.debug("Processing == expression {}", expr);

		// get our corresponding index name from the stack
		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {
			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			this.columnStack.push(null);
			return null;

		}

		// now we'll query for our column sets
		try {
			
			List<SuperColumn> greaterThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(indexKey.bumpUpValue(), indexKey.bumpUpValue() , true, MAXCOUNT),MetaDataUtils.DEFAULT);
			
			
			this.columnStack.push(convertSuperCols(greaterThan));
		

			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGteqExpression(Expression expr) {
		logger.debug("Processing >= expression {}", expr);

		// get our corresponding index name from the stack
		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {
			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			this.columnStack.push(null);
			return null;
		}

		// now we'll query for our column sets
		try {

			List<SuperColumn> greaterThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(indexKey.getIndexValue(), new byte[] {} , true, MAXCOUNT),MetaDataUtils.DEFAULT);
			
			
			this.columnStack.push(convertSuperCols(greaterThan));


			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGtExpression(Expression expr) {
		logger.debug("Processing > expression {}", expr);
		// get our corresponding index name from the stack
		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {

			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			this.columnStack.push(null);
			return null;
		}

		// now we'll query for our column sets
		try {

			List<SuperColumn> greaterThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(indexKey.bumpUpValue(), new byte[] {} , true, MAXCOUNT),MetaDataUtils.DEFAULT);
			
			
			this.columnStack.push(convertSuperCols(greaterThan));

			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLteqExpression(Expression expr) {
		logger.debug("Processing <= expression {}", expr);

		// get our corresponding index name from the stack
		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {
			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			this.columnStack.push(null);
			return null;
		}

		// now we'll query for our column sets
		try {

			List<SuperColumn> lessThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(new byte[] {}, indexKey
									.getIndexValue(), true, MAXCOUNT),MetaDataUtils.DEFAULT);

			this.columnStack.push(convertSuperCols(lessThan));
			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLtExpression(Expression expr) {
		logger.debug("Processing < expression {}", expr);

		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {
			this.columnStack.push(null);
			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			return null;
		}

		// now we'll query for our column sets
		try {

			List<SuperColumn> lessThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(new byte[] {}, indexKey
									.bumpDownValue(), true, MAXCOUNT),MetaDataUtils.DEFAULT);
			

			this.columnStack.push(convertSuperCols(lessThan));

			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processNoteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processNoteqExpression(Expression expr) {
		logger.debug("Processing != expression {}", expr);

		String indexName = getIndexNameResult();
		IndexParam indexKey = getIndexKeyResult();

		// nothing to do since we don't have an index for this parameter
		if (indexName == null || indexKey == null) {
			// do nothing, one of our left or right items require an in memory
			// evaluation and we've loaded potential keys
			if (this.columnStack.size() == 2) {
				return this.columnStack.peek();
			}

			this.columnStack.push(null);
			return null;
		}

		// now we'll query for our column sets
		try {

			List<SuperColumn> lessThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(new byte[] {}, indexKey
									.bumpDownValue(), true, MAXCOUNT),
					MetaDataUtils.DEFAULT);

			List<SuperColumn> greaterThan = selector.getSuperColumnsFromRow(
					indexName, indexKey.getIndexName(), Selector
							.newColumnsPredicate(indexKey.bumpUpValue(),
									new byte[] {}, false, MAXCOUNT),
					MetaDataUtils.DEFAULT);

			Set<Object> results = convertSuperCols(lessThan);

			convertSuperCols(greaterThan, results);

			this.columnStack.push(results);

			return this.columnStack.peek();
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processOrExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processOrExpression(Expression expr) {
		logger.debug("Processing || expression {}", expr);

		Set<Object> right = this.columnStack.pop();
		Set<Object> left = this.columnStack.pop();

		Set<Object> result = null;

		// if our left is null set the result to our right
		if (left == null) {
			result = right;
		}
		// if our left isn't null but our right is, set the result to the left
		else if (right == null) {
			result = left;
		}
		// if neither is null, perform an intersection
		else {
			// perform an intersection of the sets
			left.addAll(right);
			result = left;
		}

		// push the result back on the stack
		this.columnStack.push(result);

		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processParameterExpression
	 * (org.datanucleus.query.expression.ParameterExpression)
	 */
	@Override
	protected Object processParameterExpression(ParameterExpression expr) {
		logger.debug("Processing expression param {}", expr);

		Object value = QueryUtils.getValueForParameterExpression(
				parameterValues, expr);

		byte[] byteVal = MetaDataUtils.getIndexLong(ec, value);

		if (byteVal != null) {
			indexKeys.push(new IndexParam(MetaDataUtils.INDEX_LONG, byteVal));
			return value;
		}

		byteVal = MetaDataUtils.getIndexString(ec, value);

		if (byteVal != null) {
			indexKeys.push(new IndexParam(MetaDataUtils.INDEX_STRING, byteVal));
		}

		return value;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processPrimaryExpression
	 * (org.datanucleus.query.expression.PrimaryExpression)
	 */
	@Override
	protected Object processPrimaryExpression(PrimaryExpression expr) {
		// should be the root object return the value on the set
		logger.debug("Processing expression primary {}", expr);

		AbstractMemberMetaData member = metaData.getMetaDataForMember(expr
				.getSymbol().getQualifiedName());

		String indexName = getIndexName(metaData, member);

		if (indexName == null) {
			logger
					.warn(
							"You declared parameter {} in a query but it is not indexed",
							expr.getAlias());
			// no secondary index, so results will need to be in memory
			this.inMemoryRequired = true;
		}

		indexNames.push(indexName);

		return indexName;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral
	 * (org.datanucleus.query.expression.Literal)
	 */
	@Override
	protected Object processLiteral(Literal expr) {
		logger.debug("Processing expression primary {}", expr);

		Object value = expr.getLiteral();

		byte[] byteVal = MetaDataUtils.getIndexLong(ec, value);

		if (byteVal != null) {
			indexKeys.push(new IndexParam(MetaDataUtils.INDEX_LONG, byteVal));
			return value;
		}

		byteVal = MetaDataUtils.getIndexString(ec, value);

		if (byteVal != null) {
			indexKeys.push(new IndexParam(MetaDataUtils.INDEX_STRING, byteVal));
		}

		return value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processInvokeExpression
	 * (org.datanucleus.query.expression.InvokeExpression)
	 */
	@Override
	protected Object processInvokeExpression(InvokeExpression expr) {
		logger.debug("Processing expression invoke {}", expr);

		logger
				.warn("Processing invoke expression.  This is very inefficient!  Try to create a query without an invoke expression");

		// we don't know how this expression will be used, so we'll want to load
		// all potential keys from
		// primary table

		KeyRange keyRange = new KeyRange();
		keyRange.setStart_key("");
		keyRange.setEnd_key("");
		keyRange.setCount(MAXCOUNT);

		// don't select any columns, just keys
		try {

		
			Map<String, List<Column>> rows = selector.getColumnsFromRows(keyRange, getColumnFamily(metaData),idSelector, MetaDataUtils.DEFAULT);

			Set<Object> results = new HashSet<Object>();
			
			for(List<Column> cols: rows.values()){
				convertCols(cols, results);
			}
			
			this.columnStack.push(results);
			
		} catch (Exception e) {
			throw new NucleusException("Error processing keys", e);
		}

		this.inMemoryRequired = true;

		return this.columnStack.peek();
	}

	/**
	 * Static helper function that converts sorted maps to a merged column set
	 * 
	 * @param columns
	 * @return
	 */
	private static Set<Object> convertSuperCols(List<SuperColumn> columns) {

		Set<Object> merged = new HashSet<Object>();

		convertSuperCols(columns, merged);

		return merged;
	}

	/**
	 * Convert the columns to strings by column name and add them to the set
	 * 
	 * @param columns
	 * @param set
	 * @return
	 */
	private static void convertSuperCols(List<SuperColumn> columns,
			Set<Object> set) {

		for (SuperColumn superCol : columns) {
			for (Column col : superCol.getColumns()) {
				try {
					set.add(ByteConverter.getObject(col.getName()));
				} catch (Exception e) {
					throw new NucleusDataStoreException(
							"Unable to load serialized object identity", e);
				}
			}

		}

	}

	/**
	 * Static helper function that converts sorted maps to a merged column set
	 * 
	 * @param columns
	 * @return
	 */
	private Set<Object> convertCols(List<Column> columns) {

		Set<Object> merged = new HashSet<Object>();

		convertCols(columns, merged);

		return merged;
	}

	/**
	 * Static helper function that converts sorted maps to a merged column set
	 * 
	 * @param columns
	 * @return
	 */
	private void convertCols(List<Column> columns, Set<Object> set) {

		for (Column col : columns) {

			try {
				set.add(getObject(col.getValue()));
			} catch (Exception e) {
				throw new NucleusDataStoreException(
						"Unable to load serialized identity", e);
			}
		}

	}

	private static Set<String> convertKeysToIndexColumns(
			Map<String, List<Column>> columns) {
		Set<String> merged = new HashSet<String>();

		for (String key : columns.keySet()) {
			merged.add(key);
		}

		return merged;
	}

	/**
	 * @return the inMemoryRequired
	 */
	public boolean isInMemoryRequired() {
		return inMemoryRequired;
	}

	/**
	 * Get the index name off the stack. Will only pop if the stack sizes are
	 * equal
	 * 
	 * @return
	 */
	private String getIndexNameResult() {
		if (indexNames.size() == 0) {
			return null;
		}

		// unbalanced subtree, don't perfrom an op
		if (indexNames.size() == indexKeys.size() - 1) {
			return null;
		}

		return indexNames.pop();
	}

	/**
	 * Get the index value off the stack. Will only pop if the stack sizes are
	 * equal
	 * 
	 * @return
	 */
	private IndexParam getIndexKeyResult() {
		if (indexKeys.size() == 0) {
			return null;
		}

		if (indexKeys.size() == indexNames.size() - 1) {
			return null;
		}

		return indexKeys.pop();
	}

	/**
	 * Anything past this point isn't supported by our expression evaluator. We
	 * need to set the flag to signal to the caller to invoke the inmemory
	 * evaluator
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processAddExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processAddExpression(Expression expr) {
		logger.debug("Processing expression add {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processCaseExpression(org.datanucleus.query.expression.CaseExpression)
	 */
	@Override
	protected Object processCaseExpression(CaseExpression expr) {
		logger.debug("Processing expression case {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processCastExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processCastExpression(Expression expr) {
		logger.debug("Processing expression cast {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processComExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processComExpression(Expression expr) {
		logger.debug("Processing expression com {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processCreatorExpression
	 * (org.datanucleus.query.expression.CreatorExpression)
	 */
	@Override
	protected Object processCreatorExpression(CreatorExpression expr) {
		logger.debug("Processing expression creator {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processDistinctExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processDistinctExpression(Expression expr) {
		logger.debug("Processing expression distinct {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processDivExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processDivExpression(Expression expr) {
		logger.debug("Processing expression div {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processInExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processInExpression(Expression expr) {
		logger.debug("Processing expression in {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processIsExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processIsExpression(Expression expr) {
		logger.debug("Processing expression is {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processIsnotExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processIsnotExpression(Expression expr) {
		logger.debug("Processing is not expression {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLikeExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLikeExpression(Expression expr) {
		logger.debug("Processing expression like {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processModExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processModExpression(Expression expr) {
		logger.debug("Processing expression mod {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processMulExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processMulExpression(Expression expr) {
		logger.debug("Processing expression mult {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processNegExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processNegExpression(Expression expr) {
		logger.debug("Processing expression neg {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processNotExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processNotExpression(Expression expr) {
		logger.debug("Processing expression not {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processSubExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processSubExpression(Expression expr) {
		logger.debug("Processing expression sub {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processSubqueryExpression
	 * (org.datanucleus.query.expression.SubqueryExpression)
	 */
	@Override
	protected Object processSubqueryExpression(SubqueryExpression expr) {
		logger.debug("Processing expression subquery {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processVariableExpression
	 * (org.datanucleus.query.expression.VariableExpression)
	 */
	@Override
	protected Object processVariableExpression(VariableExpression expr) {
		logger.debug("Processing expression variable {}", expr);

		this.inMemoryRequired = true;
		this.columnStack.push(null);
		return null;
	}

	/**
	 * Helper class to wrap indexing functinality
	 * 
	 * @author Todd Nine
	 * 
	 */
	private class IndexParam {
		private String indexName;
		private byte[] indexValue;

		private IndexParam(String indexName, byte[] indexValue) {
			super();
			this.indexName = indexName;
			this.indexValue = indexValue;
		}

		/**
		 * @return the indexName
		 */
		public String getIndexName() {
			return indexName;
		}

		/**
		 * @return the indexValue
		 */
		public byte[] getIndexValue() {
			return indexValue;
		}

		/**
		 * Bump the byte value down. Equivlanet of < op
		 * 
		 * @return
		 */
		public byte[] bumpUpValue() {
			if (MetaDataUtils.INDEX_STRING.equals(indexName)) {
				return Selector
						.bumpUpColumnName(indexValue, OrderType.UTF8Type);
			}

			return Selector.bumpUpColumnName(indexValue, OrderType.LongType);
		}

		/**
		 * Bump byte value up. Equivalent of > op
		 * 
		 * @return
		 */
		public byte[] bumpDownValue() {
			if (MetaDataUtils.INDEX_STRING.equals(indexName)) {
				return Selector.bumpDownColumnName(indexValue,
						OrderType.UTF8Type);
			}

			return Selector.bumpDownColumnName(indexValue, OrderType.LongType);
		}

	}

}
