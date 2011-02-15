package com.xyz.inheritance.media;

import java.io.Serializable;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.DiscriminatorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.eaio.uuid.UUID;

@PersistenceCapable(table="Media", detachable="true")
@Inheritance(strategy=InheritanceStrategy.NEW_TABLE)
@Discriminator(strategy=DiscriminatorStrategy.VALUE_MAP, value="MediaAsset")
public class Asset implements Serializable {

	@PrimaryKey
	@Persistent(customValueStrategy = "uuid-cassandra")
	private UUID id;
	
	public UUID getId() {
		return id;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Asset other = (Asset) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	


	@Persistent
	String 		name = null;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}


}
