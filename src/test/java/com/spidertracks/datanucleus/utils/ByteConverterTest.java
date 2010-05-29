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
package com.spidertracks.datanucleus.utils;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.spidertracks.datanucleus.utils.ByteConverter;

/**
 * @author Todd Nine
 * 
 */
public class ByteConverterTest {

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getBoolean(byte[])}
	 * .
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetBoolean() throws IOException {
		boolean orig = false;

		byte[] bytes = ByteConverter.getBytes(orig);

		boolean serialized = ByteConverter.getBoolean(bytes);

		assertEquals(orig, serialized);

	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getShort(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetShort() throws IOException {
		short orig = 40;

		byte[] bytes = ByteConverter.getBytes(orig);

		short serialized = ByteConverter.getShort(bytes);

		assertEquals(orig, serialized);
	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getInt(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetInt() throws IOException {
		int orig = 65888;

		byte[] bytes = ByteConverter.getBytes(orig);

		int serialized = ByteConverter.getInt(bytes);

		assertEquals(orig, serialized);
	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getLong(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetLong() throws IOException {
		long orig = 20000;

		byte[] bytes = ByteConverter.getBytes(orig);

		long serialized = ByteConverter.getLong(bytes);

		assertEquals(orig, serialized);
	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getChar(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetChar() throws IOException {
		char orig = 'y';

		byte[] bytes = ByteConverter.getBytes(orig);

		char serialized = ByteConverter.getChar(bytes);

		assertEquals(orig, serialized);
	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getFloat(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetFloat() throws IOException {
		float orig = (float) 40.001;

		byte[] bytes = ByteConverter.getBytes(orig);

		float serialized = ByteConverter.getFloat(bytes);

		assertEquals(orig, serialized, 0);
	}

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getString(byte[])}
	 * .
	 */
	@Test
	public void testGetString() {
		String orig = "I'm a test string";

		byte[] bytes = ByteConverter.getBytes(orig);

		String serialized = ByteConverter.getString(bytes);

		assertEquals(orig, serialized);
	}
	

	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getString(byte[])}
	 * .
	 * @throws IOException 
	 */
	@Test
	public void testGetDouble() throws IOException {
		double orig = 1000.1000;

		byte[] bytes = ByteConverter.getBytes(orig);

		double serialized = ByteConverter.getDouble(bytes);

		assertEquals(orig, serialized, 0);
	}
	
	/**
	 * Test method for
	 * {@link com.spidertracks.datanucleus.utils.ByteConverter#getString(byte[])}
	 * .
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testGetObject() throws IOException, ClassNotFoundException {
		TestClass orig = new TestClass();

		byte[] bytes = ByteConverter.getBytes(orig);

		TestClass serialized =  ByteConverter.getObject(bytes);

		assertEquals(orig, serialized);
	}
	
	

}
