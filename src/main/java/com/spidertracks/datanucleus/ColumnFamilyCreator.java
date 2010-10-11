package com.spidertracks.datanucleus;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataListener;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import com.spidertracks.datanucleus.utils.MetaDataUtils;

public class ColumnFamilyCreator implements MetaDataListener {

	private Cluster cluster = null;
	private String keyspace;

	private Set<String> currentCfs = new HashSet<String>();

	private Object mutex = new Object();

	public ColumnFamilyCreator(Cluster cluster, String keyspace) {
		this.cluster = cluster;
		this.keyspace = keyspace;

		KeyspaceManager manager = Pelops.createKeyspaceManager(cluster);

		try {
			KsDef schema = manager.getKeyspaceSchema(keyspace);

			for (CfDef cf : schema.getCf_defs()) {
				currentCfs.add(cf.getName());
			}

		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Could not retrieve schema meta data", e);
		}

	}

	@Override
	public void loaded(AbstractClassMetaData cmd) {

		String cfName = MetaDataUtils.getColumnFamily(cmd);
		
		//shouldn't check the keyspace
		if(cfName == null){
			return;
		}

		if (currentCfs.contains(cfName)) {
			return;
		}

		synchronized (mutex) {

			// check again in case we're the second thread into this lock
			if (currentCfs.contains(cfName)) {
				return;
			}

			ColumnFamilyManager manager = Pelops.createColumnFamilyManager(
					cluster, keyspace);

			CfDef columnFamily = new CfDef();
			columnFamily.setName(cfName);
			columnFamily.setComparator_type("UTF8Type");
			columnFamily.setKeyspace(keyspace);

			try {
				manager.addColumnFamily(columnFamily);
			} catch (Exception e) {
				throw new NucleusDataStoreException(
						String.format("Could not create column family %s", cfName), e);
			}

			currentCfs.add(cfName);

		}

	}
}
