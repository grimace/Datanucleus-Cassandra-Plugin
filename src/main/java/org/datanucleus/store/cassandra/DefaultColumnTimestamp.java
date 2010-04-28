/**
 * 
 */
package org.datanucleus.store.cassandra;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Gets the timestamp in milliseconds in UTC timezone
 * @author Todd Nine
 *
 */
public class DefaultColumnTimestamp implements ColumnTimestamp {

	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
	
	/* (non-Javadoc)
	 * @see org.datanucleus.store.cassandra.CassandraTimeService#getTime()
	 */
	@Override
	public long getTime() {
		return Calendar.getInstance(UTC).getTimeInMillis();
	}

}
