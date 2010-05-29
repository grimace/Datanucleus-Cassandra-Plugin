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
package com.spidertracks.datanucleus;

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
