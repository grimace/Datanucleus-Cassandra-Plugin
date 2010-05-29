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
package org.datanucleus.store.cassandra;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {

	// matches the pattern cassandra:<keyspace>:host1:port, host2:port,
	// host3:port etc
	private static final Pattern URL = Pattern
			.compile("cassandra:(\\w+):(\\s*\\w+:\\d+[\\s*,\\s*\\w+:\\d+]*)");

	// create our pool
	private CassandraClientPool pool = null;

	private String keyspace = null;

	/**
	 * Constructor.
	 * 
	 * @param omfContext
	 *            The OMF context
	 * @param resourceType
	 *            Type of resource (tx, nontx)
	 */
	public ConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
		super(omfContext, resourceType);

		String connectionString = omfContext.getPersistenceConfiguration()
				.getStringProperty("datanucleus.ConnectionURL");

		// now strip off the cassandra: at the beginning and make sure our
		// format is correct

		Matcher hostMatcher = URL.matcher(connectionString);

		if (!hostMatcher.matches()) {
			throw new UnsupportedOperationException(
					"Your URL must be in the format of cassandra:host1:port[,hostN:port]");
		}

		// set our keyspace
		keyspace = hostMatcher.group(1);
		
		String hosts = hostMatcher.group(2);

		// now we're configured our cassandra hosts, put them into a pool
		CassandraHostConfigurator casHostConfigurator = new CassandraHostConfigurator(
				hosts);

		// create our pool
		this.pool = CassandraClientPoolFactory.INSTANCE
				.createNew(casHostConfigurator);

	}

	/**
	 * Obtain a connection from the Factory. The connection will be enlisted
	 * within the {@link org.datanucleus.Transaction} associated to the
	 * <code>poolKey</code> if "enlist" is set to true.
	 * 
	 * @param poolKey
	 *            the pool that is bound the connection during its lifecycle (or
	 *            null)
	 * @param options
	 *            Any options for then creating the connection
	 * @return the {@link org.datanucleus.store.connection.ManagedConnection}
	 */
	public ManagedConnection createManagedConnection(Object poolKey,
			Map transactionOptions) {

		try {
			return new CassandraManagedConnection(this.pool, this.keyspace);
		} catch (Exception e) {
			throw new NucleusException("Couldn't connect to cassandra", e);
		}


	}

	/**
	 * @return the keyspace of this connection
	 */
	public String getKeyspace() {
		return keyspace;
	}

}