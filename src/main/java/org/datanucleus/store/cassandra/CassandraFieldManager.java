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
import java.util.UUID;

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

		// TODO should we allow defaults?
		return memberMetaData.getName();

		// throw new UnsupportedOperationException(String.format(
		// "You must specify a column name for property %s",
		// memberMetaData.getName()));

	}

	/**
	 * Get the UTF8 bytes of a string
	 * 
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
	 * 
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

//	/**
//	 * As byte array.
//	 * 
//	 * @param uuid
//	 *            the uuid
//	 * 
//	 * @return the byte[]
//	 */
//	protected static byte[] getBytes(UUID uuid) {
//		long msb = uuid.getMostSignificantBits();
//		long lsb = uuid.getLeastSignificantBits();
//		byte[] buffer = new byte[16];
//
//		for (int i = 0; i < 8; i++) {
//			buffer[i] = (byte) (msb >>> 8 * (7 - i));
//		}
//		for (int i = 8; i < 16; i++) {
//			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
//		}
//
//		return buffer;
//	}
//
//	/**
//	 * Convert the uuid bytes from cassandra back to a java uuid
//	 * @param uuid
//	 * @return
//	 */
//	protected static UUID getUUID(byte[] uuid) {
//		long msb = 0;
//		long lsb = 0;
//		assert uuid.length == 16;
//		for (int i = 0; i < 8; i++)
//			msb = (msb << 8) | (uuid[i] & 0xff);
//		for (int i = 8; i < 16; i++)
//			lsb = (lsb << 8) | (uuid[i] & 0xff);
//		
//
//		// long mostSigBits = msb;
//		// long leastSigBits = lsb;
//
//		com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb, lsb);
//		return java.util.UUID.fromString(u.toString());
//	}
}
