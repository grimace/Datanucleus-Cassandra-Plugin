/**
 * 
 */
package org.datanucleus.store.cassandra;

import java.io.IOException;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.thrift.transport.TTransportException;
import org.datanucleus.store.cassandra.model.PrimitiveObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Todd Nine
 *
 */

public class BasicPersist {

	  private static EmbeddedServerHelper embedded;

	  /**
	   * Set embedded cassandra up and spawn it in a new thread.
	   *
	   * @throws TTransportException
	   * @throws IOException
	   * @throws InterruptedException
	   */
	  @BeforeClass
	  public static void setup() throws TTransportException, IOException, InterruptedException {
	    embedded = new EmbeddedServerHelper();
	    embedded.setup();
	  }

	  @AfterClass
	  public static void teardown() throws IOException {
	    embedded.teardown();
	  }

	  @Test
	  public void testBasicPerist() throws Exception {
	   
		  PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("Test");
		  PersistenceManager pm = pmf.getPersistenceManager();
		  
		  
		  PrimitiveObject objectOne = new PrimitiveObject();
		  
		  //now save our object
		  pm.makePersistent(objectOne);
	  }

}
