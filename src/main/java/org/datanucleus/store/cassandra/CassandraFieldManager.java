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

import java.io.UnsupportedEncodingException;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFieldManager extends AbstractFieldManager {

	private static final String UTF8_ENCODING = "UTF-8";

	protected static String getColumnName(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {

		AbstractMemberMetaData memberMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);

		// Try the first column if specified
		ColumnMetaData[] colmds = memberMetaData.getColumnMetaData();
		if (colmds != null && colmds.length > 0) {
			return colmds[0].getName();
		}

		else {
			throw new UnsupportedOperationException(String.format(
					"You must specify a column name for property %s",
					memberMetaData.getName()));
		}

	}
	

	
	

	/**
	 * Get the UTF8 bytes of a string
	 * @param value
	 * @return
	 */
	protected static byte[] getBytes(String value) {
		try {
			return value.getBytes(UTF8_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"I should never happen.  I'm encoding a string in UTF8");
		}
	}

	/**
	 * Convert UTF8 bytes to a string
	 * @param bytes
	 * @return
	 */
	protected static String getString(byte[] bytes) {
		try {
			return new String(bytes, UTF8_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"I should never happen.  I'm encoding a string in UTF8");
		}

	}
}
