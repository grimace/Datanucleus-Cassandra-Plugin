/**
 * 
 */
package org.datanucleus.store.cassandra.utils;

import java.util.Properties;

import org.datanucleus.store.valuegenerator.AbstractGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;


/**
 * @author Todd Nine
 * 
 */
public class CassandraUUIDGenerator extends AbstractGenerator {

	/**
	 * 
	 * @param name
	 * @param props
	 */
	public CassandraUUIDGenerator(String name, Properties props) {
		
		super(name, props);
	}

	/**
	 * Method to reserve "size" ValueGenerations to the ValueGenerationBlock.
	 * 
	 * @param size
	 *            The block size
	 * @return The reserved block
	 */
	public ValueGenerationBlock reserveBlock(long size) {
		
		
		String[] ids = new String[(int) size];
		for (int i = 0; i < size; i++) {
			ids[i] = getIdentifier();
		}
		
		return new ValueGenerationBlock(ids);
	}

	/**
	 * Create a UUID identifier.
	 * 
	 * @return The identifier
	 */
	protected String getIdentifier()
    {
		 return new com.eaio.uuid.UUID().toString();

    }
	
}
