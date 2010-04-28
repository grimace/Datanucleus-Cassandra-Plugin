/**
 * 
 */
package org.datanucleus.store.cassandra;

/**
 * This interface defines all operations required to get the time to set to column operations.  
 * 
 * @author Todd Nine
 *
 */
public interface ColumnTimestamp {

	/**
	 * Get the time in milliseconds to use when applying time stamps to columns.
	 * This is invoked once per persistence manager operation to ensure that all columns have the same time stamp when a persistent object is modified.
	 * @return
	 */
	public long getTime();
}
