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

import javax.transaction.xa.XAResource;

import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.Keyspace;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;

/**
 * Implementation of a ManagedConnection.
 */
public class CassandraManagedConnection extends AbstractManagedConnection {

	private String keySpaceName;

	private Keyspace keySpace;

	private CassandraClientPool client;

	public CassandraManagedConnection(CassandraClientPool client,
			String keySpace) {
		this.client = client;
		this.keySpaceName = keySpace;
	}

	@Override
	public void close() {
		// do nothing at the momennt
		release();
	}

	@Override
	public XAResource getXAResource() {
		// don't support xa, return null
		return null;
	}

	/**
	 * Return the hector client
	 */
	@Override
	public Object getConnection() {
		return client;
	}

	/**
	 * Get a connection to the application's keyspace with the default
	 * consistency of QUORUM
	 * 
	 * @return
	 */
	public Keyspace getKeyspace() {

		return getKeyspace(ConsistencyLevel.QUORUM);

	}

	/**
	 * Get a connection to the application's keyspace with the default
	 * consistency of QUORUM
	 * 
	 * @return
	 */
	public Keyspace getKeyspace(ConsistencyLevel consistency) {

		try {
			this.keySpace = client.borrowClient().getKeyspace(keySpaceName,
					consistency);
			return this.keySpace;
		} catch (Exception e) {

			throw new NucleusDataStoreException(String.format(
					"Unable to get a keyspace with consistencylevel %2",
					consistency), e);

		}

	}

	/**
	 * Release the connection to the current keyspace
	 */
	public void release() {
		//do nothing, the keyspace hasn't been allocated
		if (this.keySpace == null) {
			return;
		}

		try {
			this.client.releaseClient(this.keySpace.getClient());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}