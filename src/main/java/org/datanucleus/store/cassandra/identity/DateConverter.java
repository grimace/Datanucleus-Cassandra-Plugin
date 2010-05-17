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
package org.datanucleus.store.cassandra.identity;

import java.util.Date;

import org.datanucleus.StateManager;
import org.datanucleus.store.types.ObjectStringConverter;

/**
 * @author Todd Nine
 *
 */
public class DateConverter implements ObjectStringConverter {

	
	                                                                                                                                                                                                   	
	/* (non-Javadoc)
	 * @see org.datanucleus.store.types.ObjectStringConverter#toObject(java.lang.String)
	 */
	@Override
	public Object toObject(String str) {
		
		if(str == null || str.length() == 0){
			return null;
		}
		
		long epoch = Long.valueOf(str);
		
		return new Date(epoch);
		
	}

	/* (non-Javadoc)
	 * @see org.datanucleus.store.types.ObjectStringConverter#toString(java.lang.Object)
	 */
	@Override
	public String toString(Object obj) {
		
		if(!(obj instanceof Date)){
			return null;
		}
		
		return String.valueOf(((Date)obj).getTime());
	}

}
