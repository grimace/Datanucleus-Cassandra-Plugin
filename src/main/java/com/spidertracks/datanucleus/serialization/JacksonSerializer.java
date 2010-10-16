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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

/**
 * Serializer that serializes objects to JSON
 * 
 * TODO finish me, this doesn't work
 * 
 * @author Todd Nine
 * 
 */
public class JacksonSerializer implements Serializer {

	public JacksonSerializer() {

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

			ObjectMapper mapper = new ObjectMapper();
			mapper.getSerializationConfig().set(Feature.WRAP_ROOT_VALUE, true);
			
			mapper.writeValue(output, value);

			output.flush();
			output.close();

			return output.toByteArray();
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
			
			ObjectMapper om = new ObjectMapper();
			return (T) om.readValue(input, Object.class);

		} catch (IOException e) {
			throw new RuntimeException("Unable to de-serialize to json", e);
		}

	}

}
