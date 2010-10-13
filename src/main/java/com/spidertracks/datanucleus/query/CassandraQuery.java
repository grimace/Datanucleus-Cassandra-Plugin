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

Contributors : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.identity.SingleFieldIdentity;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KeyRange;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.util.ClassUtils;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.utils.ByteConverter;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

public class CassandraQuery {

	public static int search_slice_ratio = 1000; // should come from the
	// settings file

	private ExecutionContext ec;

	private Class<?> candidateClass;

	private ClassLoaderResolver clr;

	public CassandraQuery(ExecutionContext ec, Class<?> candidateClass) {
		this.ec = ec;
		this.candidateClass = candidateClass;
		this.clr = ec.getClassLoaderResolver();
	}



}
