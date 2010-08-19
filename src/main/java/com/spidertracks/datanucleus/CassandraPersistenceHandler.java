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

Contributors : Pedro Gomes and Universidade do Minho.
    		 : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.IndexPersistenceHandler.removeIndex;
import static com.spidertracks.datanucleus.utils.ByteConverter.getString;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.*;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.Relation;
import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.mutate.BatchMutationManager;
import com.spidertracks.datanucleus.mutate.ExecutionContextDelete;

public class CassandraPersistenceHandler extends AbstractPersistenceHandler {

	private CassandraStoreManager manager;
	private BatchMutationManager batchManager;

	public CassandraPersistenceHandler(CassandraStoreManager manager) {
		this.manager = manager;
		this.batchManager = new BatchMutationManager(manager);
	}

	@Override
	public void close() {

	}

	@Override
	public void deleteObject(ObjectProvider op) {
		try {

			String key = getRowKey(op);

			String columnFamily = getColumnFamily(op.getClassMetaData());

			ExecutionContext ec = op.getExecutionContext();

			ExecutionContextDelete delete = this.batchManager.beginDelete(ec,
					op);

			// we've already visited this object, do nothing
			if (!delete.addDeletion(op, key, columnFamily)) {
				return;
			}

			// delete our secondary index as well
			AbstractClassMetaData metaData = op.getClassMetaData();

			// signal a write is about to start
			Mutator mutator = this.batchManager.beginWrite(ec).getMutator();

			int[] fields = metaData.getAllMemberPositions();

			for (int current : fields) {
				AbstractMemberMetaData fieldMetaData = metaData
						.getMetaDataForManagedMemberAtAbsolutePosition(current);

				// here we have the field value
				Object value = op.provideField(current);

				if (value == null) {
					continue;
				}

				// if we're a collection, delete each element
				// recurse to delete this object if it's marked as dependent
				if (fieldMetaData.isDependent()
						|| (fieldMetaData.getCollection() != null && fieldMetaData
								.getCollection().isDependentElement())) {

					ClassLoaderResolver clr = ec.getClassLoaderResolver();

					int relationType = fieldMetaData.getRelationType(clr);

					// check if this is a relationship

					if (relationType == Relation.ONE_TO_ONE_BI
							|| relationType == Relation.ONE_TO_ONE_UNI
							|| relationType == Relation.MANY_TO_ONE_BI) {
						// Persistable object - persist the related object and
						// store the
						// identity in the cell

						ec.deleteObjectInternal(value);
					}

					else if (relationType == Relation.MANY_TO_MANY_BI
							|| relationType == Relation.ONE_TO_MANY_BI
							|| relationType == Relation.ONE_TO_MANY_UNI) {
						// Collection/Map/Array

						if (fieldMetaData.hasCollection()) {

							for (Object element : (Collection<?>) value) {
								// delete the object
								ec.deleteObjectInternal(element);
							}

						} else if (fieldMetaData.hasMap()) {
							ApiAdapter adapter = ec.getApiAdapter();

							Map<?, ?> map = ((Map<?, ?>) value);
							Object mapValue;

							// get each element and persist it.
							for (Object mapKey : map.keySet()) {

								mapValue = map.get(mapKey);

								// handle the case if our key is a persistent
								// class
								// itself
								if (adapter.isPersistable(mapKey)) {
									ec.deleteObjectInternal(mapKey);

								}
								// persist the value if it can be persisted
								if (adapter.isPersistable(mapValue)) {
									ec.deleteObjectInternal(mapValue);
								}

							}

						} else if (fieldMetaData.hasArray()) {
							Object persisted = null;

							for (int i = 0; i < Array.getLength(value); i++) {
								// persist the object
								persisted = Array.get(value, i);
								ec.deleteObjectInternal(persisted);
							}
						}

					}

				}

				IndexPersistenceHandler.removeIndex(current, value, op, mutator);

			}

			this.batchManager.endWrite(ec, DEFAULT);
			this.batchManager.endDelete(ec, DEFAULT);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void fetchObject(ObjectProvider op, int[] fieldNumbers) {
		try {
			AbstractClassMetaData metaData = op.getClassMetaData();

			String key = getRowKey(op);

			Selector selector = Pelops.createSelector(manager.getPoolName(),
					manager.getKeyspace());

			List<Column> columns = selector.getColumnsFromRow(key,
					getColumnFamily(metaData), getFetchColumnList(metaData,
							fieldNumbers), DEFAULT);

			// nothing to do
			if (columns == null || columns.size() == 0) {
				// check if the pk field was requested. If so, throw an
				// exception b/c the object doesn't exist
				pksearched(metaData, fieldNumbers);

			}

			CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
					columns, op);

			op.replaceFields(fieldNumbers, manager);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

	}

	/**
	 * Checks if a pk field was requested to be loaded. If it is null a
	 * NucleusObjectNotFoundException is thrown because we only call this with 0
	 * column results
	 * 
	 * @param metaData
	 * @param requestedFields
	 */
	private void pksearched(AbstractClassMetaData metaData,
			int[] requestedFields) {

		int[] pkPositions = metaData.getPKMemberPositions();

		for (int pkPosition : pkPositions) {
			for (int requestedField : requestedFields) {
				// our pk was a requested field, throw an exception b/c we
				// didn't find anything
				if (requestedField == pkPosition) {
					throw new NucleusObjectNotFoundException();
				}
			}
		}
	}

	@Override
	public Object findObject(ExecutionContext ec, Object id) {

		ClassLoaderResolver clr = ec.getClassLoaderResolver();

		String pcClassName = manager.getClassNameForObjectID(id, clr, ec);

		if (pcClassName == null) {
			throw new NucleusDataStoreException(
					"Object identity have an associated class");
		}

		AbstractClassMetaData metaData = ec.getMetaDataManager()
				.getMetaDataForClass(pcClassName, clr);

		SlicePredicate descriminator = getDescriminatorColumn(metaData);

		// let the core code decide what the instance should be
		if (descriminator == null) {
			return null;
		}

		String key = getRowKeyForId(ec, id);

		Object pc =  findObject(key, metaData, clr, ec, id);
		
		if(pc == null){
			//if we get to here, we couldn't find anything.  throw a not found exception
			throw new NucleusObjectNotFoundException(String.format("Could not find instance for key %s", id));
		}
		
		return pc;
	}

	private Object findObject(String key, AbstractClassMetaData metaData, ClassLoaderResolver clr, ExecutionContext ec, Object id) {

		Selector selector = Pelops.createSelector(manager.getPoolName(),
				manager.getKeyspace());

		// if we have a discriminator, fetch the discriminator column only
		// and see if it's equal
		// to the class provided by the op

		List<Column> columns = null;

		try {

			columns = selector.getColumnsFromRow(key, getColumnFamily(metaData),
					getDescriminatorColumn(metaData), DEFAULT);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

		// what do we do if no descriminator is found and one should be
		// present?
		if (columns == null || columns.size() != 1) {

			// now check if we have subclasses from the given metaData, if we do
			// recurse to a child class and search for the object
			String[] decendents = ec.getMetaDataManager().getSubclassesForClass(metaData.getFullClassName(), true);

			//it has decendents, only recurse to them if their inheritance strategy is a new table
			if (decendents == null || decendents.length == 0) {
				return null;
			}
			
			
			AbstractClassMetaData decendentMetaData = null;
			
			for(String decendent: decendents){
				decendentMetaData = ec.getMetaDataManager().getMetaDataForClass(decendent, clr);
				
				InheritanceStrategy strategy = decendentMetaData.getInheritanceMetaData().getStrategy();
				
				//either the subclass has it's own table, or one if it's children may, recurse to find the object
				if(InheritanceStrategy.NEW_TABLE.equals(strategy) || InheritanceStrategy.SUBCLASS_TABLE.equals(strategy)){
					Object result = findObject(key, decendentMetaData, clr, ec, id);
					
					// we found a subclass with the descriminator stored, return it
					if(result != null){
						return result;
					}
				}
			}
			
			//nothing found in this class or it's children return null
			return null;

		}

		String descriminatorValue = getString(columns.get(0).getValue());

		String className = org.datanucleus.metadata.MetaDataUtils
				.getClassNameFromDiscriminatorValue(descriminatorValue,
						metaData.getDiscriminatorMetaData(), ec);

		// now recursively load the search for our class

		Class<?> newObjectClass = ec.getClassLoaderResolver().classForName(
				className);

		ObjectProvider sm = ObjectProviderFactory.newForHollow(ec,
				newObjectClass, id);

		Object pc = sm.getObject();

		ObjectProvider pcSM = ec.findObjectProvider(pc);
		if (pcSM == null) {
			sm.replaceManagedPC(pc);
		}

		return pc;
	}

	@Override
	public void insertObject(ObjectProvider op) {
		// just delegate to update. They both perform the same logic
		updateObject(op, op.getClassMetaData().getAllMemberPositions());

	}

	@Override
	public void locateObject(ObjectProvider op) {
		fetchObject(op, op.getClassMetaData().getAllMemberPositions());

	}

	@Override
	public void updateObject(ObjectProvider op, int[] fieldNumbers) {

		this.manager.assertReadOnlyForUpdateOfObject(op);

		AbstractClassMetaData metaData = op.getClassMetaData();

		ExecutionContext ec = op.getExecutionContext();

		// signal a write is about to start
		Mutator mutator = this.batchManager.beginWrite(ec).getMutator();

		String key = getRowKey(op);
		String columnFamily = getColumnFamily(metaData);

		// Write our all our primary object data
		CassandraInsertFieldManager manager = new CassandraInsertFieldManager(
				mutator, op, columnFamily, key);

		op.provideFields(metaData.getAllMemberPositions(), manager);

		// if we have a discriminator, write the value
		if (metaData.hasDiscriminatorStrategy()) {
			DiscriminatorMetaData discriminator = metaData
					.getDiscriminatorMetaData();

			String colName = getDiscriminatorColumnName(discriminator);

			String value = discriminator.getValue();

			mutator.writeColumn(key, columnFamily, mutator.newColumn(colName,
					value));
		}

		try {

			this.batchManager.endWrite(ec, DEFAULT);

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

	}

}