r/**********************************************************************
Copyright (c) 2010 Todd Nine and others. All rights reserved.
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

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Keyspace;

import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;

public class CassandraPersistenceHandler extends AbstractPersistenceHandler
{
	
	private CassandraStoreManager manager;
	
	public CassandraPersistenceHandler(CassandraStoreManager manager){
		this.manager = manager;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteObject(ObjectProvider op) {
		
	}

	@Override
	public void fetchObject(ObjectProvider op, int[] fieldNumbers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object findObject(ExecutionContext ectx, Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertObject(ObjectProvider op) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void locateObject(ObjectProvider op) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject(ObjectProvider op, int[] fieldNumbers) {
		// TODO Auto-generated method stub
		
	}
	
	private Keyspace getKeyspace(ObjectProvider op){
		CassandraManagedConnection conn = (CassandraManagedConnection) manager.getConnection(op.getExecutionContext());
		String keySpace = ((ConnectionFactoryImpl)manager.getConnectionFactory2()).getKeyspace();
		
		return conn.cassandraClient().getKeyspace(keySpace);	
	}
   
    
}