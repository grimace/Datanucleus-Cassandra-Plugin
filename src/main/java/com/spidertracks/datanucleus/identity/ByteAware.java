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
package com.spidertracks.datanucleus.identity;

import java.nio.ByteBuffer;

/**
 * Interface that complex objects can implement to generate their own bytes for storage in either
 * columns or as row keys.  Note that each implementation must have a default empty constructor
 * 
 * @author Todd Nine
 *
 */
public interface ByteAware {

	/**
	 * Create a byte buffer and write all bytes to it for storage
	 * @return
	 */
	public ByteBuffer getBytes();
	
	/**
	 * Populate a new instance of the object from the bytes
	 * @param buffer
	 */
	public void parseBytes(ByteBuffer buffer);
	

	/**
	 * Returns the comparator type (UTF-8, TimeUUIDType etc) that should be used to compare values
	 * Only used with secondary indexing
	 * @return
	 */
	public String getComparatorType();
}
