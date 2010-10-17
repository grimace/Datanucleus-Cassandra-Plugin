package com.spidertracks.datanucleus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.types.TypeManager;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * Create column families and index columns if they do not exist when a class is loaded the first time.
 * 
 * @author Todd Nine
 *
 */
public class ColumnFamilyCreator implements MetaDataListener {

	private Cluster cluster = null;
	private String keyspace;

	private Set<String> existingCfs = new HashSet<String>();
	private Set<String> visitedCfs = new HashSet<String>();

	private StoreManager storeManager;

	private Object mutex = new Object();
	private boolean createColumns;

	public ColumnFamilyCreator(StoreManager storeManager, Cluster cluster,
			String keyspace, boolean createColumns) {
		this.storeManager = storeManager;
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.createColumns = createColumns;

		KeyspaceManager manager = Pelops.createKeyspaceManager(cluster);

		try {
			KsDef schema = manager.getKeyspaceSchema(keyspace);

			for (CfDef cf : schema.getCf_defs()) {
				existingCfs.add(cf.getName());
			}

		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Could not retrieve schema meta data", e);
		}

	}

	@Override
	public void loaded(AbstractClassMetaData cmd) {

		String cfName = MetaDataUtils.getColumnFamily(cmd);

		// shouldn't check the keyspace
		if (cfName == null) {
			return;
		}

		if (visitedCfs.contains(cfName)) {
			return;
		}

		synchronized (mutex) {
			// could be the second into the mutex
			if (visitedCfs.contains(cfName)) {
				return;
			}

			ColumnFamilyManager manager = Pelops.createColumnFamilyManager(
					cluster, keyspace);

			CfDef columnFamily = new CfDef(keyspace, cfName);
			columnFamily.setComparator_type("UTF8Type");

			// now go through the corresponding fields and create our indexes

			if (createColumns) {
				
				List<ColumnDef> indexColumns = new ArrayList<ColumnDef>(
						cmd.getAllMemberPositions().length);

				TypeManager typeManager = storeManager.getOMFContext()
						.getTypeManager();

				for (int field : cmd.getAllMemberPositions()) {
					AbstractMemberMetaData memberData = cmd
							.getMetaDataForManagedMemberAtAbsolutePosition(field);

					String indexName = MetaDataUtils.getIndexName(cmd,
							memberData);

					if (indexName == null) {
						continue;
					}

					String columnName = MetaDataUtils.getColumnName(cmd, field);

					String validationClass = MetaDataUtils.getValidationClass(
							memberData.getType(), typeManager);

					ColumnDef def = new ColumnDef(Bytes.fromUTF8(columnName)
							.getBytes(), validationClass);

					def.setIndex_name(indexName);
					def.setIndex_type(IndexType.KEYS);

					indexColumns.add(def);

				}

				// if we have a discriminator we should index it as we'll be
				// referencing it in queries
				String discriminatorColumn = MetaDataUtils
						.getDiscriminatorColumnName(cmd);

				if (discriminatorColumn != null) {
					ColumnDef def = new ColumnDef(Bytes.fromUTF8(
							discriminatorColumn).getBytes(),
							ColumnFamilyManager.CFDEF_COMPARATOR_UTF8);
					def.setIndex_name(discriminatorColumn + "_index");
					def.setIndex_type(IndexType.KEYS);
					indexColumns.add(def);
				}

				columnFamily.setColumn_metadata(indexColumns);

			}

			try {
				if (existingCfs.contains(cfName)) {
					manager.updateColumnFamily(columnFamily);
				} else {
					manager.addColumnFamily(columnFamily);
				}
			} catch (Exception e) {
				throw new NucleusDataStoreException(String.format(
						"Could not create column family %s", cfName), e);
			}

			visitedCfs.add(cfName);

		}

	}

}
