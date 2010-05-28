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
package me.prettyprint.cassandra.testutils;

import java.io.IOException;

import org.apache.thrift.transport.TTransportException;


/**
 * @author Todd Nine
 * 
 */
public enum CassandraServer {

	INSTANCE;

	private EmbeddedServerHelper defaultPool;

	private CassandraServer() {
	}

	public static CassandraServer getInstance() {
		return INSTANCE;
	}

	/**
	 * Get a reference to a reusable pool.
	 * 
	 * @return
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws TTransportException 
	 */
	public void start() throws TTransportException, IOException, InterruptedException {
		if (defaultPool == null) {
			synchronized (INSTANCE) {
				if (defaultPool == null) {
					defaultPool = new EmbeddedServerHelper();
					defaultPool.setup();
				}
			}
		}
		
	}
	
	public void stop(){
		defaultPool.teardown();
	}
	
	
	

	

}
