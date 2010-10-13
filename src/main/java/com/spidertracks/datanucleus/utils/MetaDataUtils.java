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
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.utils;

import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jdo.identity.ObjectIdentity;
import javax.jdo.identity.SingleFieldIdentity;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.exceptions.DatastoreFieldDefinitionException;
import org.datanucleus.store.types.ObjectLongConverter;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.TypeManager;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.Selector;

import com.eaio.uuid.UUID;

/**
 * Utility class to convert instance data to Cassandra columns and data types
 * 
 * TODO Cache the data returned here for better performance
 * 
 * @author Todd Nine
 * 
 */
public class MetaDataUtils {

	public static final ConsistencyLevel DEFAULT = ConsistencyLevel.ONE;

	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String INDEX_LONG = "LongIndex";
	public static final String INDEX_STRING = "StringIndex";

	private static ConcurrentMap<String, String> classToCfNames = new ConcurrentHashMap<String, String>();
	private static ConcurrentMap<IndexMetaData, String> fieldToIndexNames = new ConcurrentHashMap<IndexMetaData, String>();

	//
	/**
	 * Convenience method to find an object given a string form of its identity,
	 * and the metadata for the class (or a superclass).
	 * 
	 * @param idStr
	 *            The id string
	 * @param cmd
	 *            Metadata for the class
	 * @return The object
	 */
	public static String getRowKey(ObjectProvider op) {

		return getRowKey(op.getExecutionContext(), op.getObject());

	}

	/**
	 * Get the stringified key for the given execution context and object
	 * 
	 * @param ec
	 * @param object
	 * @return
	 */
	public static String getRowKey(ExecutionContext ec, Object object) {
		Object id = ec.getApiAdapter().getIdForObject(object);

		return getRowKeyForId(ec, id);
	}

	/**
	 * Get the row key for the given id
	 * 
	 * @param ec
	 * @param id
	 * @return
	 */
	public static String getRowKeyForId(ExecutionContext ec, Object id) {
		if (id instanceof ObjectIdentity) {
			ObjectIdentity identity = (ObjectIdentity) id;

			ObjectStringConverter converter = ec.getTypeManager()
					.getStringConverter(identity.getKey().getClass());

			if (converter == null) {
				throw new DatastoreFieldDefinitionException(String.format(
						"You must define an ObjectStringConverter for type %s",
						identity.getKey().getClass()));
			}

			return converter.toString(identity.getKey());
		} else if (id instanceof SingleFieldIdentity) {

			SingleFieldIdentity identity = (SingleFieldIdentity) id;

			ObjectStringConverter converter = ec.getTypeManager()
					.getStringConverter(identity.getKeyAsObject().getClass());

			if (converter != null) {
				return converter.toString(identity.getKeyAsObject());
			}

		}

		// else just call the default tostring since it's a user defined key
		return id.toString();
	}

	


	/**
	 * Use the datanucleus converers to convert from the string to a new
	 * instance of the target class
	 * 
	 * @param ec
	 * @param targetClass
	 * @param value
	 * @return
	 */
	public static Object getIdentityFromRowKey(ExecutionContext ec,
			String targetClassName, String value) {

		ClassLoaderResolver clr = ec.getClassLoaderResolver();

		Class<?> targetClass = clr.classForName(targetClassName);

		if (targetClass.equals(String.class)) {
			return value;
		}

		ObjectStringConverter converter = ec.getTypeManager()
				.getStringConverter(targetClass);

		if (converter == null) {
			throw new DatastoreFieldDefinitionException(String.format(
					"You must define an ObjectStringConverter for type %s",
					targetClass));
		}

		return converter.toObject(value);

	}

	/**
	 * Object for the identity in the AbstractClassMetaData's class. Try and
	 * build it from the string
	 * 
	 * @param op
	 * @param cmd
	 * @param value
	 * @return
	 */
	public static Object getObjectIdentity(ExecutionContext ec,
			Class<?> candidateClass, byte[] value) {

		ApiAdapter adapter = ec.getApiAdapter();

		AbstractClassMetaData cmd = ec.getMetaDataManager()
				.getMetaDataForClass(candidateClass,
						ec.getClassLoaderResolver());

		if (!adapter.isSingleFieldIdentityClass(cmd.getObjectidClass())) {
			throw new DatastoreFieldDefinitionException(
					"multiple field identity creation is currently unsupported in indexing");
		}

		int[] identityFields = cmd.getPKMemberPositions();

		// assume only one identity field

		AbstractMemberMetaData member = cmd
				.getMetaDataForManagedMemberAtAbsolutePosition(identityFields[0]);

		try {

			// if the class of the pk is a primitive we'll want to get the value
			// then set it as a string

			ObjectStringConverter converter = ec.getTypeManager()
					.getStringConverter(member.getType());

			Bytes bytes = new Bytes(value);

			// use the converter if it's present
			if (converter != null) {
				Object id = converter.toObject(bytes.toUTF8());

				return ec.newObjectId(candidateClass, id);
			}

			ObjectLongConverter longConverter = ec.getTypeManager()
					.getLongConverter(member.getType());

			if (longConverter != null) {
				Object id = longConverter.toObject(bytes.toLong());

				return ec.newObjectId(candidateClass, id);
			}

			// check if it's a primitive type
			if (member.getType().equals(Boolean.class)) {
				return ec.newObjectId(candidateClass, bytes.toBoolean());
			}

			else if (member.getType().equals(Short.class)) {
				return ec.newObjectId(candidateClass, bytes.toShort());
			}

			else if (member.getType().equals(Integer.class)) {
				return ec.newObjectId(candidateClass, bytes.toInt());
			}

			else if (member.getType().equals(Long.class)) {
				return ec.newObjectId(candidateClass, bytes.toLong());
			}

			else if (member.getType().equals(Double.class)) {
				return ec.newObjectId(candidateClass, bytes.toDouble());
			}

			else if (member.getType().equals(Float.class)) {
				return ec.newObjectId(candidateClass, bytes.toFloat());
			}

			else if (member.getType().equals(String.class)) {
				return ec.newObjectId(candidateClass, bytes.toUTF8());
			}

		
			return ByteConverter.getObject(bytes.getBytes());
		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Unable to serialize bytes to object identity.  Please make sure it has the same SerializationId, long or string converter is was stored with. ");
		}

	

	}

	/**
	 * Get the column metadata for the class and fieldname
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static String getColumnName(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {

		AbstractMemberMetaData memberMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);

		// Try the first column if specified
		ColumnMetaData[] colmds = memberMetaData.getColumnMetaData();
		if (colmds != null && colmds.length > 0) {
			return colmds[0].getName();
		}

		// TODO should we allow defaults?
		return memberMetaData.getName();

		// throw new UnsupportedOperationException(String.format(
		// "You must specify a column name for property %s",
		// memberMetaData.getName()));

	}

	/**
	 * Get the column metadata for the class and fieldname
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static String getIdentityColumn(AbstractClassMetaData metaData) {

		int[] pks = metaData.getPKMemberPositions();

		if (pks.length != 1) {
			throw new NucleusDataStoreException(
					"Currently only single field identity objects are allowed");
		}

		AbstractMemberMetaData memberMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(pks[0]);

		// Try the first column if specified
		ColumnMetaData[] colmds = memberMetaData.getColumnMetaData();
		if (colmds != null && colmds.length > 0) {
			return colmds[0].getName();
		}

		// TODO should we allow defaults?
		return memberMetaData.getName();

	}

	/**
	 * Get the column name from the meta data, if it's not specified the default
	 * name of "classtype" is returned
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getDiscriminatorColumnName(
			DiscriminatorMetaData metaData) {
		String name = metaData.getColumnName();

		if (name == null) {
			name = "classtype";
		}

		return name;
	}

	/**
	 * Get the name of the index. Will return null if no index is defined. If
	 * one is, it takes the name assigned by the user, otherwise it will create
	 * an index in the format of <TableName>_<FieldName>
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getIndexName(AbstractClassMetaData classMetaData,
			AbstractMemberMetaData fieldMetaData) {

		IndexMetaData metaData = fieldMetaData.getIndexMetaData();

		if (metaData == null) {
			return null;
		}

		
		String name = fieldToIndexNames.get(metaData);
		
		if(name != null){
			return name;
		}
				
		
		name = metaData.getName();

		if (name == null) {
			StringBuffer nameBuffer = new StringBuffer();

			nameBuffer.append(fieldMetaData.getName()).append("_").append("index");
			name = nameBuffer.toString();
		}
		
		fieldToIndexNames.putIfAbsent(metaData, name);

		return name;

	}

	/**
	 * Get the byte value of the column names
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static byte[] getColumnNameBytes(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {

		return getColumnName(metaData, absoluteFieldNumber).getBytes(UTF8);

	}

	/**
	 * Get the column path to the entire class
	 * 
	 * @param metaData
	 * @return
	 */
	public static ColumnPath getClassColumnFamily(AbstractClassMetaData metaData) {
		return new ColumnPath(getColumnFamily(metaData));

	}

	/**
	 * Get the column parent for the given class metadata. Matches the
	 * "table name" on the class meta data
	 * 
	 * @param op
	 * @return
	 */
	public static ColumnParent getColumnParent(AbstractClassMetaData metaData) {
		return new ColumnParent(getColumnFamily(metaData));
	}

	/**
	 * Get the name of the column family. Uses table name, if one doesn't exist,
	 * it uses the simple name of the class. If this class should never be
	 * persisted directly (subclass table persistence) null is returned
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getColumnFamily(AbstractClassMetaData metaData) {

		String passedClassName = metaData.getFullClassName();
		String cfName = classToCfNames.get(passedClassName);

		if (cfName != null) {
			return cfName;
		}

		AbstractClassMetaData current = metaData;
		InheritanceMetaData inheritance = null;
		String tableName = null;

		while (current != null) {
			tableName = current.getTable();

			if (tableName != null) {
				cfName = tableName;
				break;
			}

			inheritance = metaData.getInheritanceMetaData();

			if (inheritance != null) {

				InheritanceStrategy strategy = inheritance.getStrategy();

				if (InheritanceStrategy.NEW_TABLE.equals(strategy)) {
					cfName = metaData.getTable();

					if (cfName == null) {
						cfName = metaData.getEntityName();

					}

					break;
				}

				// this class should never be persisted directly, return null
				else if (InheritanceStrategy.SUBCLASS_TABLE.equals(strategy)) {
					return null;
				}

			}

			current = (AbstractClassMetaData) current
					.getSuperAbstractClassMetaData();

		}

		classToCfNames.put(passedClassName, cfName);

		return cfName;

	}

	/**
	 * Create a slice predicate with all mapped fetch column lists
	 * 
	 * @param metaData
	 * @param fieldNumbers
	 * @return
	 */
	public static SlicePredicate getFetchColumnList(
			AbstractClassMetaData metaData, int[] fieldNumbers) {

		String[] fieldNames = new String[fieldNumbers.length];

		for (int i = 0; i < fieldNumbers.length; i++) {
			fieldNames[i] = getColumnName(metaData, fieldNumbers[i]);
		}

		return Selector.newColumnsPredicate(fieldNames);
	}

	/**
	 * Create a slice predicate that will retreive the discriminator column if
	 * one doesn't exist, null is returned
	 * 
	 * @param metaData
	 * @param fieldNumbers
	 * @return
	 */
	public static SlicePredicate getDescriminatorColumn(
			AbstractClassMetaData metaData) {

		DiscriminatorMetaData discriminatorMetaData = metaData
				.getDiscriminatorMetaData();

		if (discriminatorMetaData == null) {
			return null;
		}

		String columnName = getDiscriminatorColumnName(discriminatorMetaData);

		return Selector.newColumnsPredicate(columnName);
	}
	
	/**
	 * Return the cassandra validator for the class specified.
	 * @param fieldClass
	 * @return
	 */
	public static String getValidationClass(Class<?> fieldClass, TypeManager manager){
		
		if (Boolean.class.equals(fieldClass)) {
			return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
		}

		if (String.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
		}

		if (Short.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
		}

		if (Integer.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_INTEGER;
		}
		
		if (Double.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_INTEGER;
		}

		if (Long.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
		}

		if (Float.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
		}

		if (java.util.UUID.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_TIME_UUID;
		}

		if (UUID.class.equals(fieldClass)){
			return ColumnFamilyManager.CFDEF_COMPARATOR_LEXICAL_UUID;
		}

		
		ObjectLongConverter converter = manager.getLongConverter(fieldClass);

		if (converter != null) {
			return ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
		}

		
		
		ObjectStringConverter stringConverter = manager.getStringConverter(fieldClass);
		
		if(stringConverter != null){
			return ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
		}
		
		
		
		throw new NucleusDataStoreException(
				"Could not convert object to byte.  Object types must be of any primitive, a UTF8 String, or com.eaio.uuid.UUID/java.util.UUID");
	}

}
