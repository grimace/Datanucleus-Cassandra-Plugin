package com.spidertracks.datanucleus;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.spidertracks.datanucleus.array.ArrayTests;
import com.spidertracks.datanucleus.basic.PrimitiveTests;
import com.spidertracks.datanucleus.collection.CollectionTests;
import com.spidertracks.datanucleus.map.MapTests;

@RunWith(Suite.class)
@SuiteClasses( { ArrayTests.class, CollectionTests.class, MapTests.class,
		PrimitiveTests.class })
public class AllTests {
	
	
	
	

}
