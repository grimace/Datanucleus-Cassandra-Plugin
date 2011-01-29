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
package com.spidertracks.datanucleus.convert;

import org.datanucleus.store.types.ObjectLongConverter;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * Encapsulates both a long converter and a DN long converter in the same instance
 * @author Todd Nine
 *
 */
public class ObjectLongWrapperConverter implements ByteConverter {

	private ObjectLongConverter dnLongConverter;
	private ByteConverter longConverter;
	
	public ObjectLongWrapperConverter(ObjectLongConverter dnLongConverter, ByteConverter longConverter){
		this.dnLongConverter = dnLongConverter;
		this.longConverter = longConverter;
	}
	
	@Override
	public Object getObject(Bytes bytes) {
		if(bytes == null){
			return null;
		}
		return dnLongConverter.toObject((Long)longConverter.getObject(bytes));
	}

	@Override
	public Bytes getBytes(Object value) {
		return longConverter.getBytes(dnLongConverter.toLong(value));
	}

	@Override
	public String getComparatorType() {
		return ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
	}

	

}
