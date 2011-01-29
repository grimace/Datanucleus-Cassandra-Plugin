package com.spidertracks.datanucleus.convert;

import org.scale7.cassandra.pelops.Bytes;

/**
 * Interface that allows conversions of objects to bytes
 * @author Todd Nine
 *
 */
public interface ByteConverter {

	/**
	 * Read the bytes from the byte buffer and return the object
	 * @param buffer
	 * @return
	 */
	public Object getObject(Bytes bytes);
	
	
	/**
	 * Get the byte buffer for the given instance
	 * @param instance
	 * @return
	 */
	public Bytes getBytes(Object value);
	
	
	/**
	 * Returns the comparator type (UTF-8, TimeUUIDType etc) that should be used
	 * @return
	 */
	public String getComparatorType();
	
}
