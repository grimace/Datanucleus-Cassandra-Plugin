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
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.ByteConverter.getBytes;

import java.io.IOException;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.wyki.cassandra.pelops.Mutator;

import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * @author Todd Nine
 *
 */
public class IndexPersistenceHandler {

	/**
	 * Index the field if required
	 * @param fieldNumber The field number
	 * @param value The value of the field
	 * @param objectProvider The object provider
	 * @param mutator The mutator to perform the write op in 
	 */
	public static void indexField(int fieldNumber, Object value, ObjectProvider objectProvider, Mutator mutator) {
		
		AbstractClassMetaData metaData = objectProvider.getClassMetaData();
		ExecutionContext context = objectProvider.getExecutionContext();
		
		// convert it to a string so we can key it

		AbstractMemberMetaData fieldMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

		String indexName = MetaDataUtils.getIndexName(metaData, fieldMetaData);

		// nothing to index
		if (indexName == null) {
			return;
		}

		try {
			byte[] data = MetaDataUtils.getIndexLong(context, value);

			// Long indexible
			if (data != null ) {
				
				//No usable value to write
				if(data.length == 0){
					return;
				}

				mutator.writeSubColumn(indexName, MetaDataUtils.INDEX_LONG,
						data, mutator.newColumn(getBytes(objectProvider
								.getObjectId()), new byte[] { 0x00 }));

				return;
			}

			data = MetaDataUtils.getIndexString(context, value);

			if (data != null) {
				
				//No usable value to write
				if(data.length == 0){
					return;
				}
				
				
				mutator.writeSubColumn(indexName, MetaDataUtils.INDEX_STRING,
						data, mutator.newColumn(getBytes(objectProvider
								.getObjectId()), new byte[] { 0x00 }));

			}

		} catch (IOException e) {
			throw new NucleusDataStoreException("Unable to write indexes", e);
		}

	}
	

	/**
	 * Removes the index value if one exists
	 * @param fieldNumber
	 * @param value
	 * @param objectProvider
	 * @param mutator
	 */
	public static void removeIndex(int fieldNumber, ObjectProvider objectProvider, Mutator mutator) {
		
		AbstractClassMetaData metaData = objectProvider.getClassMetaData();
		ExecutionContext context = objectProvider.getExecutionContext();

		AbstractMemberMetaData fieldMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

		String indexName = MetaDataUtils.getIndexName(metaData,
				fieldMetaData);

		// nothing to index
		if (indexName == null) {
			return;
		}

		// load the original value from the DS to un-index it
		try {
			objectProvider.loadFieldFromDatastore(fieldNumber);
		} catch (NucleusDataStoreException ne) {
			// our field didn't exist previously, ignore the error
			return;
		}

		Object oldValue = objectProvider.provideField(fieldNumber);

		// no old value, nothing to do
		if (oldValue == null) {
			return;
		}
		
		try {
			
			byte[] data = MetaDataUtils.getIndexLong(context, oldValue);

			// Long indexible
			if (data != null) {
				
				//No usable value to write
				if(data.length == 0){
					return;
				}

				mutator.deleteSubColumn(indexName,  MetaDataUtils.INDEX_LONG, data, getBytes(objectProvider.getObjectId()));

				return;
			}

			data = MetaDataUtils.getIndexString(context, oldValue);

			if (data != null) {
				//No usable value to write
				if(data.length == 0){
					return;
				}
				
				mutator.deleteSubColumn(indexName,  MetaDataUtils.INDEX_STRING, data, getBytes(objectProvider.getObjectId()));

			}
		} catch (IOException e) {
			throw new NucleusDataStoreException("Unable to write indexes", e);
		}


	}

}
