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

Contributors : Todd Nine

***********************************************************************/
package com.spidertracks.datanucleus;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Policy;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {

	// matches the pattern cassandra:<poolname>:<keyspace>:<connectionport>:host1, host2, host3... etc
	private static final Pattern URL = Pattern
			.compile("cassandra:(\\w+):(\\w+):(\\d+):(\\s*\\w+[.\\w+]*[\\s*,\\s*\\w+[.\\w+]*]*)");

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
					"Your URL must be in the format of cassandra:poolname:keyspace:port:host1[,hostN");
		}

		//pool name
		String poolName = hostMatcher.group(1);
		
		// set our keyspace
		String keyspace = hostMatcher.group(2);
		
		//grab our port
		int defaultPort = Integer.parseInt(hostMatcher.group(3));
		
		String[] hosts = hostMatcher.group(4).split(",");
		
		//by default we won't discover other nodes we're not explicitly connected to.  May change in future
		Pelops.addPool(poolName, hosts, defaultPort, false, keyspace, new Policy());
		
		CassandraStoreManager manager = (CassandraStoreManager)omfContext.getStoreManager();
		manager.setKeyspace(keyspace);
		manager.setPoolName(poolName);
		

	
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
	@SuppressWarnings("unchecked")
	public ManagedConnection createManagedConnection(Object poolKey,
			Map transactionOptions) {

//		try {
//			return new CassandraManagedConnection(this.keyspace);
//		} catch (Exception e) {
//			throw new NucleusException("Couldn't connect to cassandra", e);
//		}

		throw new NucleusDataStoreException("Not supported");

	}

	

}