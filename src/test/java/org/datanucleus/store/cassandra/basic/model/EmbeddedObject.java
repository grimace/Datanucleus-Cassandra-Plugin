/**
 * 
 */
package org.datanucleus.store.cassandra.basic.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import org.datanucleus.store.cassandra.model.BaseEntity;



/**
 * @author Todd Nine
 * Basic object for testing primitive persistence.  Only support types in java.lang
 * 
 */
@PersistenceCapable(table = "PrimitiveObject", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class EmbeddedObject extends BaseEntity{

	@javax.jdo.annotations.Embedded
	private Embedded embedded = new Embedded();
	


	@PersistenceCapable
	public static class Embedded{
		
	}

}
