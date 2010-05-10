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
public class UUIDGenerator extends AbstractGenerator {

	public UUIDGenerator(String name, Properties props) {
		super(name, props);
	}

	@Override
	protected ValueGenerationBlock reserveBlock(long size) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Class getStorageClass() {
		return byte[].class;
	}

}
