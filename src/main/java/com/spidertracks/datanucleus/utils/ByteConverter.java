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

import org.datanucleus.store.types.ObjectLongConverter;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.TypeManager;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

import com.eaio.uuid.UUID;

/**
 * Utility class to convert data types to byte streams
 * 
 * @author Todd Nine
 * 
 */
public class ByteConverter {

	
	
	/**
	 * Converts the value to it's underlying type. Converts all primitives and
	 * UUID. Does not serialize objects as this is intended to be used for
	 * querying
	 * 
	 * @param value
	 * @return
	 */
	public static Bytes convertToStorageType(Object value, TypeManager manager) {
		if (value instanceof Boolean) {
			return Bytes.fromBoolean((Boolean) value);
		}

		if (value instanceof String) {
			return Bytes.fromUTF8((String) value);
		}

		if (value instanceof Short) {
			return Bytes.fromShort((Short) value);
		}

		if (value instanceof Integer) {
			return Bytes.fromDouble((Integer) value);
		}
		
		if (value instanceof Double) {
			return Bytes.fromDouble((Double) value);
		}

		if (value instanceof Long) {
			return Bytes.fromLong((Long) value);
		}

		if (value instanceof Float) {
			return Bytes.fromFloat((Float) value);
		}

		if (value instanceof java.util.UUID) {
			return Bytes.fromUuid((java.util.UUID) value);
		}

		if (value instanceof UUID) {
			long high = ((UUID) value).getClockSeqAndNode();
			long low = ((UUID) value).getTime();

			return Bytes.fromUuid(high, low);
		}

		
		ObjectLongConverter converter = manager.getLongConverter(value
				.getClass());

		if (converter != null) {
			return Bytes.fromLong(converter.toLong(value));
		}

		
		
		ObjectStringConverter stringConverter = manager.getStringConverter(value.getClass());
		
		if(stringConverter != null){
			return Bytes.fromUTF8(stringConverter.toString(value));
		}
		
		
		
		throw new RuntimeException(
				"Could not convert object to byte.  Object types must be of any primitive, a UTF8 String, or com.eaio.uuid.UUID/java.util.UUID");
	}

	/**
	 * Write to byte array
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(Object value) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(value);
			oos.flush();

			byte[] bytes = bos.toByteArray();

			oos.close();
			bos.close();

			return bytes;
		} catch (Exception e) {
			throw new RuntimeException("Unable to serialize to object", e);
		}
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
	public static <T> T getObject(byte[] bytes) {
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInputStream ois;

			ois = new ObjectInputStream(bis);

			T serialized = (T) ois.readObject();
			ois.close();
			bis.close();

			return serialized;
		} catch (Exception e) {
			throw new RuntimeException("Unable to de-serialize to object", e);
		}
	}

}