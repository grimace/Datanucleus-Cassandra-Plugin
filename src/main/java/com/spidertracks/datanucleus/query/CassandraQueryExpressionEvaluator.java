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

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getIndexName;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.datanucleus.ClassLoaderResolver;
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
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spidertracks.datanucleus.query.runtime.AndOperand;
import com.spidertracks.datanucleus.query.runtime.CompressableOperand;
import com.spidertracks.datanucleus.query.runtime.EqualityOperand;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.query.runtime.OrOperand;
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

	
	
	private static final Logger logger = LoggerFactory
			.getLogger(CassandraQueryExpressionEvaluator.class);
	
	private static final int MAX = 1000;

	private Stack<IndexParam> indexKeys = new Stack<IndexParam>();
	private Stack<Operand> operationStack = new Stack<Operand>();

	private AbstractClassMetaData metaData;

	// Flag that marks an operation we can't support has been performed. We need
	// to then run result set
	// through the memory scanner
	private boolean inMemoryRequired;

	/** Map of input parameter values, keyed by their name. */
	private Map<String, Object> parameterValues;

	private ExecutionContext ec;

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

		inMemoryRequired = false;

		// assume only one identity field

		

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

		//get our current left and right
		Operand left = operationStack.pop();
		Operand right = operationStack.pop();
		
		//compress the right and left on this && into a single statement for efficiency
		if(left instanceof CompressableOperand && right instanceof CompressableOperand){
			EqualityOperand op = new EqualityOperand(MAX);
			
			op.addAll(((CompressableOperand)left).getIndexClause().getExpressions());
			
			op.addAll(((CompressableOperand)right).getIndexClause().getExpressions());
			
			return operationStack.push(op);
		}
		
		//we can't compress, just add the left and right
		AndOperand op = new AndOperand();
		op.setLeft(left);
		op.setRight(right);

		return operationStack.push(op);
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
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.EQ,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(expression);

		return this.operationStack.push(op);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGteqExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.GTE,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGtExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.GT,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLteqExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.LTE,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLtExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.LT,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(expression);

		return this.operationStack.push(op);

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

		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression less = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.LT,
				indexKey.getIndexValue());
		IndexExpression greater = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.GT,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(MAX);
		op.addExpression(less);
		op.addExpression(greater);

		return this.operationStack.push(op);
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
		
		//get our current left and right
		Operand left = operationStack.pop();
		Operand right = operationStack.pop();
		
	
		
		//we can't compress, just add the left and right
		OrOperand op = new OrOperand();
		op.setLeft(left);
		op.setRight(right);

		return operationStack.push(op);
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

		Bytes byteVal = MetaDataUtils.getIndexLong(ec, value);

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

		IndexParam param = new IndexParam(indexName, null);

		return indexKeys.push(param);

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

		Bytes byteVal = MetaDataUtils.getIndexLong(ec, value);
		
		IndexParam param = indexKeys.peek();

		if (byteVal != null) {
			param.setIndexValue(byteVal);
			return value;
		}

		byteVal = MetaDataUtils.getIndexString(ec, value);

		if (byteVal != null) {
			param.setIndexValue(byteVal);
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

		logger.error("Processing invoke expression.  This is very inefficient!  Try to create a query without an invoke expression");
		//
		return null;
		// logger.debug("Processing expression invoke {}", expr);
		//
		// logger
		// .warn("Processing invoke expression.  This is very inefficient!  Try to create a query without an invoke expression");
		//
		// // we don't know how this expression will be used, so we'll want to
		// load
		// // all potential keys from
		// // primary table
		//
		// KeyRange keyRange = new KeyRange();
		// keyRange.setStart_key(Bytes.EMPTY.getBytes());
		// keyRange.setEnd_key(Bytes.EMPTY.getBytes());
		// keyRange.setCount(MAXCOUNT);
		//
		// // don't select any columns, just keys
		// try {
		//
		// Map<Bytes, List<Column>> rows = selector.getColumnsFromRows(
		// getColumnFamily(metaData), keyRange, idSelector,
		// MetaDataUtils.DEFAULT);
		//
		// Set<Object> results = new HashSet<Object>();
		//
		// for (List<Column> cols : rows.values()) {
		// convertCols(cols, results);
		// }
		//
		// this.columnStack.push(results);
		//
		// } catch (Exception e) {
		// throw new NucleusException("Error processing keys", e);
		// }
		//
		// this.inMemoryRequired = true;
		//
		// return this.columnStack.peek();
	}

	/**
	 * @return the inMemoryRequired
	 */
	public boolean isInMemoryRequired() {
		return inMemoryRequired;
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		// this.columnStack.push(null);
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
		private Bytes indexValue;

		private IndexParam(String indexName, Bytes indexValue) {
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
		public Bytes getIndexValue() {
			return indexValue;
		}

		/**
		 * 
		 * @param indexValue
		 */
		public void setIndexValue(Bytes indexValue) {
			this.indexValue = indexValue;
		}

	}

}
