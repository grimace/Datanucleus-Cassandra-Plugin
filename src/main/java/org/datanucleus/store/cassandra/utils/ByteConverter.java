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
package org.datanucleus.store.cassandra.utils;

import java.io.UnsupportedEncodingException;

/**
 * @author Todd Nine
 *
 */
public class ByteConverter {

	private static final String UTF8_ENCODING = "UTF-8";

	
	/**
	 * Get the UTF8 bytes of a string
	 * 
	 * @param value
	 * @return
	 */
	public static byte[] getBytes(String value) {
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
	public static String getString(byte[] bytes) {
		try {
			return new String(bytes, UTF8_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"I should never happen.  I'm encoding a string in UTF8");
		}

	}
}
