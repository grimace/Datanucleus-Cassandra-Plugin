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
package org.datanucleus.store.cassandra;

import static org.datanucleus.store.cassandra.utils.ByteConverter.getBytes;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.cassandra.mutate.BatchMutationManager;

/**
 * @author Todd Nine
 * 
 */
public class CassandraInsertFieldManager extends CassandraFieldManager {

//	private List<Column> updates;
//	private List<SuperColumn> superColumns;
//	private List<Deletion> deletes;
	private ExecutionContext context;
	private BatchMutationManager manager;
	private AbstractClassMetaData metaData;
	private StateManager stateManager;
	private String columnFamily;
	private String rowKey;
	private long timestamp;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(BatchMutationManager manager,  StateManager stateManager, String tableName, String rowKey, long updateTimestamp) {
		super();

		this.manager = manager;
		this.stateManager = stateManager;
		this.metaData = stateManager.getClassMetaData();
		this.context = stateManager.getObjectProvider().getExecutionContext();

//		this.updates = updates;
//		this.superColumns = superColumns;
//		this.deletes = deletes;
		this.columnFamily = tableName;
		this.rowKey = rowKey;
		this.timestamp = updateTimestamp;
		

	}

	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			
			manager.AddColumn(context, columnFamily, rowKey, columnName, getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName, new byte[] { value }, timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);

		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}
			
			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);
			manager.AddColumn(context, columnFamily, rowKey, columnName,getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	// these values can be deleted, so we'll want to handle deletes if they're
	// null

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
//		try {
//
//			if (isKey(fieldNumber)) {
//				return;
//			}
//
//			// TODO make cascading saves happen
//			// return;
//
//			ObjectProvider op = stateManager.getObjectProvider();
//			ExecutionContext ec = op.getExecutionContext();
//
//			ClassLoaderResolver clr = ec.getClassLoaderResolver();
//			AbstractMemberMetaData fieldMetaData = stateManager
//					.getClassMetaData()
//					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
//			int relationType = fieldMetaData.getRelationType(clr);
//
//			String columnName = getColumnName(metaData, fieldNumber);
//
//			// delete operation
//			if (value == null) {
//
//				SlicePredicate slicePredicate = new SlicePredicate();
//				slicePredicate.addToColumn_names(getBytes(columnName));
//				Deletion delete = new Deletion(this.updateTimestamp);
//
//				delete.setPredicate(slicePredicate);
//
//				this.deletes.add(delete);
//				return;
//
//			}
//
//			// check if this is a relationship
//			else if (relationType == Relation.ONE_TO_ONE_BI
//					|| relationType == Relation.ONE_TO_ONE_UNI
//					|| relationType == Relation.MANY_TO_ONE_BI) {
//				// Persistable object - persist the related object and store the
//				// identity in the cell
//				if (fieldMetaData.isEmbedded()) {
//					// TODO Handle embedded objects
//					throw new NucleusDataStoreException(
//							"Embedded objects are currently unimplemented.");
//					// Class embcls = fieldMetaData.getType();
//					// AbstractClassMetaData embcmd = ec.getMetaDataManager()
//					// .getMetaDataForClass(embcls, clr);
//					// if (embcmd != null) {
//					// ObjectProvider efop = null;
//					// if (value != null) {
//					// efop = ec.findObjectProviderForEmbedded(value, op,
//					// fieldMetaData);
//					// } else {
//					// efop = ec.newObjectProviderForMember(fieldMetaData,
//					// embcmd);
//					// }
//					//
//					// FieldManager ffm = new StoreEmbeddedFieldManager(efop,
//					// row, fieldMetaData);
//					// efop.provideFields(embcmd.getAllMemberPositions(), ffm);
//					// return;
//					// }
//				}
//
//				Object valuePC = op.getExecutionContext()
//						.persistObjectInternal(value, op, fieldNumber, -1);
//				Object valueId = op.getExecutionContext().getApiAdapter()
//						.getIdForObject(valuePC);
//
//				// TODO add this data to the supercolumn info
//				
//
//				// add it to a super column on this object
//			} else if (relationType == Relation.MANY_TO_MANY_BI	|| relationType == Relation.ONE_TO_MANY_BI	|| relationType == Relation.ONE_TO_MANY_UNI) {
//				// Collection/Map/Array
//				if (fieldMetaData.hasCollection()) {
//					StringBuffer cellValue = new StringBuffer("[");
//					Collection coll = (Collection) value;
//					Iterator collIter = coll.iterator();
//					while (collIter.hasNext()) {
//						Object element = collIter.next();
//						Object elementPC = sm.getExecutionContext()
//								.persistObjectInternal(element, sm,
//										fieldNumber, -1);
//						Object elementID = sm.getExecutionContext()
//								.getApiAdapter().getIdForObject(elementPC);
//						cellValue.append(elementID.toString());
//						if (collIter.hasNext()) {
//							cellValue.append(",");
//						}
//					}
//					cellValue.append("]");
//					CreationHelper createHelper = row.getSheet().getWorkbook()
//							.getCreationHelper();
//					cell.setCellValue(createHelper
//							.createRichTextString(cellValue.toString()));
//				} else if (fieldMetaData.hasMap()) {
//					// TODO Implement map persistence - what to do if key or
//					// value is non-PC
//					throw new NucleusException(
//							"Dont currently support persistence of map types to Excel");
//				} else if (fieldMetaData.hasArray()) {
//					StringBuffer cellValue = new StringBuffer("[");
//					for (int i = 0; i < Array.getLength(value); i++) {
//						Object element = Array.get(value, i);
//						Object elementPC = sm.getExecutionContext()
//								.persistObjectInternal(element, sm,
//										fieldNumber, -1);
//						Object elementID = sm.getExecutionContext()
//								.getApiAdapter().getIdForObject(elementPC);
//						cellValue.append(elementID.toString());
//						if (i < (Array.getLength(value) - 1)) {
//							cellValue.append(",");
//						}
//					}
//					cellValue.append("]");
//					CreationHelper createHelper = row.getSheet().getWorkbook()
//							.getCreationHelper();
//					cell.setCellValue(createHelper
//							.createRichTextString(cellValue.toString()));
//				}
//			}
//			//			
//			//			
//			// //it's a collection. Create a super column and add all the keys
//			// that are created to it.
//			// if(value instanceof Collection){
//			//				
//			// Stack<Column> columns = new Stack<Column>();
//			//				
//			// for (Object element : (Collection)value) {
//			// stateManager.getObjectManager().persistObject(element);
//			//					
//			// //TODO get the pk for the object
//			//					
//			//					
//			// StateManager elemStateManager =
//			// stateManager.getObjectManager().findStateManager(element);
//			//					
//			//					
//			// String key = getKey(elemStateManager);
//			//					
//			// // stateManager.
//			//					
//			//					
//			// columns.push(new Column(getBytes("c"), getBytes(key),
//			// this.updateTimestamp));
//			// }
//			//				
//			//				
//			// this.superColumns.add(new SuperColumn(getBytes(columnName),
//			// columns));
//			//				
//			// }
//			//			
//			// default case where we persist raw objects
//
//			Column column = new Column(getBytes(columnName), getBytes(value),
//					this.updateTimestamp);
//
//			updates.add(column);
//
//		} catch (Exception e) {
//			throw new NucleusException(e.getMessage(), e);
//		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {

			if (isKey(fieldNumber)) {
				return;
			}

			String columnName = getColumnName(metaData, fieldNumber);

			if (value == null) {
				manager.AddDelete(context, columnFamily,  rowKey, columnName, fieldNumber);
				return;
			}

			manager.AddColumn(context, columnFamily, rowKey, columnName, getBytes(value), timestamp);


		} catch (Exception e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	protected ColumnPath getColumnPath(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {
		ColumnPath columnPath = new ColumnPath(metaData.getTable());
		columnPath.setColumn(getBytes(getColumnName(metaData,
				absoluteFieldNumber)));

		return columnPath;
	}

	/**
	 * Returns true if the field is a key
	 * 
	 * @param fieldNumber
	 * @return
	 */
	protected boolean isKey(int fieldNumber) {

		return metaData.getMetaDataForManagedMemberAtAbsolutePosition(
				fieldNumber).isPrimaryKey();

	}

}
