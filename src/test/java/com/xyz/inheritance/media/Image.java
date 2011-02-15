package com.xyz.inheritance.media;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.DiscriminatorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable(detachable="true")
@Inheritance(strategy=InheritanceStrategy.SUPERCLASS_TABLE)
@Discriminator(strategy=DiscriminatorStrategy.VALUE_MAP, value="MediaImage")
public class Image extends Asset {
	
	@Persistent
	String imageType;

	public String getImageType() {
		return imageType;
	}

	public void setImageType(String imageType) {
		this.imageType = imageType;
	}

}
