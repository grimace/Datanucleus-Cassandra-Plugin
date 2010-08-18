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

import static com.spidertracks.datanucleus.utils.ByteConverter.getString;

import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.identity.ObjectIdentity;
import javax.jdo.identity.SingleFieldIdentity;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.exceptions.DatastoreFieldDefinitionException;
import org.datanucleus.store.types.ObjectStringConverter;
import org.wyki.cassandra.pelops.Selector;

/**
 * Utility class to convert instance data to Cassandra columns and data types
 * 
 * TODO Cache the data returned here for better performance
 * 
 * @author Todd Nine
 * 
 */
public class MetaDataUtils {

	public static final ConsistencyLevel DEFAULT = ConsistencyLevel.DCQUORUM;

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
	 * @param ec
	 * @param id
	 * @return
	 */
	public static String getRowKeyForId(ExecutionContext ec, Object id){
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
	 * Convert from the given object to a string.
	 * 
	 * @param ec
	 * @param o
	 * @return
	 */
	public static String convertToRowKey(ExecutionContext ec, Object o) {

		if (o instanceof String) {
			return (String) o;
		}

		ObjectStringConverter converter = ec.getTypeManager()
				.getStringConverter(o.getClass());

		if (converter == null) {
			throw new DatastoreFieldDefinitionException(String.format(
					"You must define an ObjectStringConverter for type %s", o
							.getClass()));
		}

		return converter.toString(o);
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
	public static Object convertFromRowKeyString(ExecutionContext ec,
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

	// /**
	// * Object for the identity in the AbstractClassMetaData's class. Try and
	// * build it from the string
	// *
	// * @param op
	// * @param cmd
	// * @param value
	// * @return
	// */
	// public static Object getKeyValue(ObjectProvider op,
	// AbstractClassMetaData cmd, String value) {
	//
	// ExecutionContext ec = op.getExecutionContext();
	//
	// ApiAdapter adapter = ec.getApiAdapter();
	//
	// Class<?> persistentClass = ec.getClassLoaderResolver().classForName(
	// cmd.getFullClassName());
	//
	// Object identity = null;
	//
	// if (adapter.isSingleFieldIdentityClass(cmd.getObjectidClass())) {
	//
	// int[] identityFields = cmd.getPKMemberPositions();
	//			
	// //assume only one identity field
	//			
	// AbstractMemberMetaData member =
	// cmd.getMetaDataForManagedMemberAtAbsolutePosition(identityFields[0]);
	//			
	// identity = ec.newObjectId(persistentClass, member.getTypeName()
	// + ":" + value);
	//
	// } else {
	// throw new DatastoreFieldDefinitionException(
	// "multiple field identity creation is currently unsupported");
	// }
	//
	// return identity;
	//
	// }

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

		String assignedName = metaData.getName();

		if (assignedName == null) {
			StringBuffer nameBuffer = new StringBuffer();
			nameBuffer.append(getColumnFamily(classMetaData)).append("_")
					.append(fieldMetaData.getName());
			assignedName = nameBuffer.toString();
		}

		return assignedName;

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

		return ByteConverter.getBytes(getColumnName(metaData,
				absoluteFieldNumber));

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
	 * it uses the simple name of the class
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getColumnFamily(AbstractClassMetaData metaData) {
		AbstractClassMetaData current = metaData;
		InheritanceMetaData inheritance = null;
		String tableName = null;;
		
		while (current != null) {
			tableName = current.getTable();

			if (tableName != null) {
				return tableName;
			}
			
			inheritance = metaData.getInheritanceMetaData();
			
			if(inheritance != null){
				if(InheritanceStrategy.NEW_TABLE.equals(inheritance.getStrategy()) || InheritanceStrategy.UNSPECIFIED.equals(inheritance.getStrategy())){
					return metaData.getName();
				}
			}

			current = (AbstractClassMetaData) current.getSuperAbstractClassMetaData();

		}

		//couldn't determine name via inheritance or annotations, default tl class name
		return metaData.getName();
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
	 * Create a slice predicate that will retreive the discriminator column
	 * if one doesn't exist, null is returned
	 * 
	 * @param metaData
	 * @param fieldNumbers
	 * @return
	 */
	public static SlicePredicate getDescriminatorColumn(AbstractClassMetaData metaData) {

		DiscriminatorMetaData discriminatorMetaData = metaData.getDiscriminatorMetaData();
		
		if(discriminatorMetaData == null){
			return null;
		}
		
		String columnName = getDiscriminatorColumnName(discriminatorMetaData);
			
		
		return Selector.newColumnsPredicate(columnName);
	}

}
