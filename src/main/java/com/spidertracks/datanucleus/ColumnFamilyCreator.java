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
 * Create column families and index columns if they do not exist when a class is
 * loaded the first time.
 * 
 * @author Todd Nine
 * 
 */
public class ColumnFamilyCreator implements MetaDataListener {

	private Cluster cluster = null;
	private String keyspace;

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

			KeyspaceManager keyspaceManager = Pelops
					.createKeyspaceManager(cluster);
			KsDef schema = null;

			try {
				schema = keyspaceManager.getKeyspaceSchema(keyspace);
			} catch (Exception e) {
				throw new NucleusDataStoreException(
						"Could not retrieve schema meta data", e);
			}

			ColumnFamilyManager manager = Pelops.createColumnFamilyManager(
					cluster, keyspace);

			CfDef columnFamily = getDefinition(cfName, schema);

			boolean existingCf = true;

			// no column family, define one
			if (columnFamily == null) {
				columnFamily = new CfDef(keyspace, cfName);
				columnFamily.setComparator_type("UTF8Type");
				existingCf = false;
			}

			// now go through the corresponding fields and create our indexes

			int previousColumnCount = 0;
			int newColumnCount = 0;

			if (createColumns) {

				List<ColumnDef> indexColumns = columnFamily
						.isSetColumn_metadata() ? columnFamily
						.getColumn_metadata() : new ArrayList<ColumnDef>();

				previousColumnCount = indexColumns.size();

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

					// already defined
					if (hasColumn(columnName, columnFamily)) {
						continue;
					}

					String validationClass = MetaDataUtils.getValidationClass(
							memberData.getType(), typeManager);

					ColumnDef def = new ColumnDef();
					
					def.setName(Bytes.fromUTF8(columnName).getBytes());
					def.setValidation_class(validationClass);

					def.setIndex_name(indexName);
					def.setIndex_type(IndexType.KEYS);

					indexColumns.add(def);

				}

				// if we have a discriminator we should index it as we'll be
				// referencing it in queries
				String discriminatorColumn = MetaDataUtils
						.getDiscriminatorColumnName(cmd);

				if (discriminatorColumn != null
						&& !hasColumn(discriminatorColumn, columnFamily)) {
					ColumnDef def = new ColumnDef();
					
					def.setName(Bytes.fromUTF8(
							discriminatorColumn).getBytes());
					def.setValidation_class(ColumnFamilyManager.CFDEF_COMPARATOR_UTF8);
					
					def.setIndex_name(discriminatorColumn + "_index");
					def.setIndex_type(IndexType.KEYS);
					indexColumns.add(def);
				}

				columnFamily.setColumn_metadata(indexColumns);

				newColumnCount = indexColumns.size();

			}

			try {
				if (existingCf) {
					if (previousColumnCount != newColumnCount) {
						manager.updateColumnFamily(columnFamily);
					}
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

	/**
	 * Return the cf definition if it exists. If not null is returned
	 * 
	 * @param cfName
	 * @param keyspace
	 * @return
	 */
	private CfDef getDefinition(String cfName, KsDef keyspace) {
		for (CfDef cf : keyspace.getCf_defs()) {
			if (cfName.equals(cf.getName())) {
				return cf;
			}
		}

		return null;
	}

	/**
	 * Returns true if the column exists, false otherwise
	 * 
	 * @param cfName
	 * @param cf
	 * @return
	 */
	private boolean hasColumn(String cfName, CfDef cf) {
		if (!cf.isSetColumn_metadata()) {
			return false;
		}

		Bytes colName = Bytes.fromUTF8(cfName);

		for (ColumnDef col : cf.getColumn_metadata()) {
			if (colName.equals(new Bytes(col.getName()))) {
				return true;
			}
		}

		return false;
	}

}
