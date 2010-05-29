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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.NucleusConnection;

public class CassandraStoreManager extends AbstractStoreManager
{
	
	//    MetaDataListener metadataListener;
 
    private boolean autoCreateTables = false;
    private boolean autoCreateColumns = false;

    private int poolTimeBetweenEvictionRunsMillis; 
    private int poolMinEvictableIdleTimeMillis;
    
    private ColumnTimestamp columnTimestamp;
   
	/**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param omfContext ObjectManagerFactory context
     */
    public CassandraStoreManager(ClassLoaderResolver clr, OMFContext omfContext)
    {
        super("cassandra", clr, omfContext);
        
 
        // Handler for persistence process
        persistenceHandler = new CassandraPersistenceHandler(this);


        PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
        boolean autoCreateSchema = conf.getBooleanProperty("datanucleus.autoCreateSchema");
        
//       Cassandra can't do this until 0.7
        if (autoCreateSchema)
        {
            autoCreateTables = true;
            autoCreateColumns = true;
        }
        else
        {
            autoCreateTables = conf.getBooleanProperty("datanucleus.autoCreateTables");
            autoCreateColumns = conf.getBooleanProperty("datanucleus.autoCreateColumns");
        }        
        // how often should the evictor run
        poolTimeBetweenEvictionRunsMillis = conf.getIntProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis");
        
        if (poolTimeBetweenEvictionRunsMillis == 0)
        {
            poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
        }
         
        // how long may a connection sit idle in the pool before it may be evicted
        poolMinEvictableIdleTimeMillis = conf.getIntProperty("datanucleus.connectionPool.minEvictableIdleTimeMillis");
        
        if (poolMinEvictableIdleTimeMillis == 0)
        {
            poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
        }
        
        String timeClassName = conf.getStringProperty("datanucleus.store.cassandra.timestamp");
        
        //try and load the class they've specified
        if(timeClassName != null){
        	try {
				this.columnTimestamp = (ColumnTimestamp) this.getClass().getClassLoader().loadClass(timeClassName).newInstance();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }else{
        	this.columnTimestamp = new DefaultColumnTimestamp();
        }
      
       
        
        logConfiguration();
    }

    protected void registerConnectionMgr()
    {
        super.registerConnectionMgr();
        this.connectionMgr.disableConnectionPool();
    }


    /**
     * Release of resources
     */
    public void close()
    {
//        omfContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext om)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        //could happen if writing to "one" or reading from "one" node
        set.add("TransactionIsolationLevel.read-uncommitted");
        return set;
    }
    
    
    public ColumnTimestamp getTimestamp() {
		return columnTimestamp;
	}

    

    
    public boolean isAutoCreateColumns()
    {
        return autoCreateColumns;
    }
    
    public boolean isAutoCreateTables()
    {
        return autoCreateTables;
    }
    
    public int getPoolMinEvictableIdleTimeMillis()
    {
        return poolMinEvictableIdleTimeMillis;
    }
    
    public int getPoolTimeBetweenEvictionRunsMillis()
    {
        return poolTimeBetweenEvictionRunsMillis;
    }
}
