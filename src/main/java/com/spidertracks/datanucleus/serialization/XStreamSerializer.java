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
package com.spidertracks.datanucleus.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

/**
 * Serializer that serializes objects to JSON
 * 
 * @author Todd Nine
 * 
 */
public class XStreamSerializer implements Serializer {

	public XStreamSerializer() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.serialization.Serializer#getBytes(java.lang
	 * .Object)
	 */
	@Override
	public byte[] getBytes(Object value) {
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			
			XStream xstream = new XStream();

			ObjectOutputStream oos = xstream.createObjectOutputStream(output);
			
			oos.writeObject(value);
			oos.flush();
			oos.close();
			
			
			output.flush();

			byte[] result  = output.toByteArray();
			
			output.close();
			
			return result;
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize to json", e);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.serialization.Serializer#getObject(byte[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(byte[] bytes) {

		try {
			
			ByteArrayInputStream input = new ByteArrayInputStream(bytes);
			
			XStream xstream = new XStream();

			ObjectInputStream ois = xstream.createObjectInputStream(input);
			
			
			T result =  (T) ois.readObject();
			
			input.close();
			ois.close();
			
			return result;
			
		} catch (Exception e) {
			throw new RuntimeException("Unable to de-serialize to json", e);
		}

	}

}
