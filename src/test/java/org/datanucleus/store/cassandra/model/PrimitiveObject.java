/**
 * 
 */
package org.datanucleus.store.cassandra.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;



/**
 * @author Todd Nine
 * 
 */
@PersistenceCapable(table = "PrimitiveObject", identityType = IdentityType.APPLICATION)
public class PrimitiveObject {

	@PrimaryKey
	@Persistent(customValueStrategy = "uuid-cassandra")
	//@Persistent(valueStrategy =  IdGeneratorStrategy.UUIDHEX)
	private String id;

	@Persistent
	private boolean testBool;


	public boolean isTestBool() {
		return testBool;
	}

	public void setTestBool(boolean testBool) {
		this.testBool = testBool;
	}

	

	public String getId() {
		return id;
	}

}
