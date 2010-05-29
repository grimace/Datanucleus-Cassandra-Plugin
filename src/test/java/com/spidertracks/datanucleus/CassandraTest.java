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

import java.io.IOException;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author Todd Nine
 *
 */
public abstract class CassandraTest {


	
	protected static PersistenceManagerFactory pmf;
	

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException,
			InterruptedException {
		
		CassandraServer.INSTANCE.start();
		
		pmf = JDOHelper.getPersistenceManagerFactory("Test");
	}

	@AfterClass
	public static void teardown() throws IOException {
		//no shutdown for now
	}
}
