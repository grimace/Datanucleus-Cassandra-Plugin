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
package com.spidertracks.datanucleus.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;

import org.wyki.cassandra.pelops.NumberHelper;

/**
 * Utility class to convert data types to byte streams
 * 
 * @author Todd Nine
 * 
 */
public class ByteConverter {

	private static final String UTF8_ENCODING = "UTF-8";

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(boolean value) throws IOException {
		short saved = (short) (value ? 1 : 0);

		return NumberHelper.toBytes(saved);

	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static boolean getBoolean(byte[] bytes) throws IOException {
		short saved = NumberHelper.toShort(bytes);

		return saved == 1;
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(short value) throws IOException {
		return NumberHelper.toBytes(value);
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static short getShort(byte[] bytes) throws IOException {
		return NumberHelper.toShort(bytes);
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(int value) throws IOException {
		return NumberHelper.toBytes(value);
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static int getInt(byte[] bytes) throws IOException {
		return NumberHelper.toInt(bytes);
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(long value) throws IOException {
		return NumberHelper.toBytes(value);
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static long getLong(byte[] bytes) throws IOException {
		return NumberHelper.toLong(bytes);
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(char value) throws IOException {
		return getBytes(new String(new char[] { value }));
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static char getChar(byte[] bytes) throws IOException {
		return getString(bytes).charAt(0);
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(float value) throws IOException {
		return getBytes(Float.floatToIntBits(value));
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static float getFloat(byte[] bytes) throws IOException {
		return Float.intBitsToFloat(NumberHelper.toInt(bytes));
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(double value) throws IOException {
		return NumberHelper.toBytes(Double.doubleToLongBits(value));
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static double getDouble(byte[] bytes) throws IOException {
		return Double.longBitsToDouble(NumberHelper.toLong(bytes));
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(Object value) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(value);
		oos.flush();

		byte[] bytes = bos.toByteArray();

		oos.close();
		bos.close();

		return bytes;
	}

	/**
	 * Read byte array to boolean
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getObject(byte[] bytes) throws IOException,
			ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bis);
		T serialized = (T) ois.readObject();
		ois.close();
		bis.close();

		return serialized;
	}

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
