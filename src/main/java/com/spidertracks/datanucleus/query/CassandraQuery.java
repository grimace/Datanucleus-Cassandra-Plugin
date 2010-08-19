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
package com.spidertracks.datanucleus.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KeyRange;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ExecutionContext;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.utils.ByteConverter;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

public class CassandraQuery {

	public static int search_slice_ratio = 1000; // should come from the
													// settings file

	/**
	 * Used to load all keys from a given persistence row
	 */
	@SuppressWarnings("unchecked")
	public static List getObjectsOfCandidateType(final ExecutionContext ec,
			Class candidateClass, boolean subclasses, boolean ignoreCache,
			int limit, String startKey) {

		try {

			CassandraStoreManager manager = ((CassandraStoreManager) ec
					.getStoreManager());

			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			String columnFamily = MetaDataUtils.getColumnFamily(acmd);

			Selector selector = Pelops.createSelector(manager.getPoolName(),
					manager.getKeyspace());

			KeyRange range = new KeyRange();
			range.setCount(limit);

			// TODO TN set up our range keys
			range.setStart_key("");
			range.setEnd_key("");
		

			String identityColumn = MetaDataUtils.getIdentityColumn(acmd);
			
			// This behavior is somewhat undetermined. We have no idea what
			// we're looking for. If there are
			// subclass tables, we need to keep loading keys until we hit our
			// limit if there are table per subclass entities.  Using this is woefully inneficient, and generally a very bad idea.  You should have defined secondary indexes
			// and searched those.  If you need to get everything, you probably shouldn't be using JDO to access this data unless the set size is very small
			Map<String, List<Column>> rows = selector.getColumnsFromRows(range,
					columnFamily, Selector.newColumnsPredicate(identityColumn),
					MetaDataUtils.DEFAULT);

			
			Set<Object> keys = new HashSet<Object>(rows.size());
			
			for(List<Column> entries: rows.values()){
				
				//deleted row, ignore it
				if(entries == null || entries.size() != 1){
					continue;
				}
				
				Object identity = MetaDataUtils.getObjectIdentity(ec, candidateClass, entries.get(0).getValue());
				
				
			
				keys.add(identity);
			}
			
			return getObjectsOfCandidateType(ec, candidateClass, keys, subclasses, ignoreCache );

		} catch (Exception e) {
			throw new NucleusDataStoreException("Unable to load results", e);
		}

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
	public static List<?> getObjectsOfCandidateType(final ExecutionContext ec,
			Class<?> candidateClass, Set<Object> keys, boolean subclasses,
			boolean ignoreCache) {
		

		//final ClassLoaderResolver clr = ec.getClassLoaderResolver();
		//final AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
		
		List results = new ArrayList(keys.size());
		//String tempKey = null;

		for (Object id : keys) {
			
			Object returned = ec.findObject(id, ignoreCache, subclasses, candidateClass.getName());

			if (returned != null) {
				results.add(returned);
			}
		}
		
		return results;

	}

	

	// /**
	// * Fetches all of the column data into objects
	// *
	// * @param rows
	// * @param ec
	// * @param acmd
	// * @param clr
	// * @return
	// */
	// @SuppressWarnings("unchecked")
	// private static List<?> loadResults(Map<String, List<Column>> rows,
	// final ExecutionContext ec, final AbstractClassMetaData acmd,
	// final ClassLoaderResolver clr) {
	//
	// List results = new ArrayList(rows.size());
	//
	// for (final List<Column> columnsForKey : rows.values()) {
	//
	// //This is due to eventual consistency
	// //removed records could be returned, so we'll ignore them
	// if(columnsForKey.size() == 0){
	// continue;
	// }
	//			
	// results.add(ec.findObjectUsingAID(new Type(clr.classForName(acmd
	// .getFullClassName())), new FieldValues2() {
	//
	// @Override
	// public FetchPlan getFetchPlanForLoading() {
	// return null;
	// }
	//
	// @Override
	// public void fetchNonLoadedFields(ObjectProvider sm) {
	// sm.replaceNonLoadedFields(sm.getDirtyFieldNumbers(),
	// new CassandraFetchFieldManager(columnsForKey, sm));
	// }
	//
	// @Override
	// public void fetchFields(ObjectProvider sm) {
	// sm.replaceFields(acmd.getAllMemberPositions(),
	// new CassandraFetchFieldManager(columnsForKey, sm));
	//					
	//
	// }
	// }, true, true));
	//
	// }
	//
	// return results;
	// }
}
