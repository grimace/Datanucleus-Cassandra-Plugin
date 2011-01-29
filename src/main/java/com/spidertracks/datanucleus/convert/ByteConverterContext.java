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

Contributors :
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.convert;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.jdo.identity.ObjectIdentity;
import javax.jdo.identity.SingleFieldIdentity;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.exceptions.DatastoreFieldDefinitionException;
import org.datanucleus.store.types.ObjectLongConverter;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.TypeManager;
import org.scale7.cassandra.pelops.Bytes;

import com.spidertracks.datanucleus.identity.ByteAware;
import com.spidertracks.datanucleus.serialization.Serializer;

/**
 * Context per Store Manager that allows conversion of data types to native
 * bytes. Stored the mappings to allow users to define their own conversion
 * scheme. The converter uses the default byte encoding for all Java primitives.
 * All Strings are encode with UTF 8. To create your own converters, define a
 * properties file like the following and put it into your classpath
 * 
 * FileName: cassandraByteCoverters.properties
 * 
 * Format:
 * 
 * <pre>
 * <classname>:<converter classname>
 * 
 * </pre>
 * 
 * Then in your JDO configuration add the following properties
 * 
 * <pre>
 * <?xml version="1.0" encoding="utf-8"?>
 * <jdoconfig xmlns="http://java.sun.com/xml/ns/jdo/jdoconfig"
 * 	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 * 	xsi:noNamespaceSchemaLocation="http://java.sun.com/xml/ns/jdo/jdoconfig">
 * 
 * 	<!-- Datastore Txn PMF -->
 * 	<persistence-manager-factory name="Test">
 * 		<property name="javax.jdo.PersistenceManagerFactoryClass"
 * 			value="org.datanucleus.jdo.JDOPersistenceManagerFactory" />
 * 		<property name="javax.jdo.option.ConnectionURL" value="cassandra:TestPool:true:false:10000:TestingKeyspace:19160:127.0.0.1" />
 * 		<property name="javax.jdo.option.Optimistic" value="false" />
 * 		
 * 		<!--  auto creates keyspace and CF.  Not recommended for production environments since replication factor and strategy are defaulted to 1 and simple-->
 * 		<property name="datanucleus.autoCreateSchema" value="true" />
 * 		
 * 		<property name="datanucleus.autoCreateTables" value="true"/>
 * 		
 * 		<property name="datanucleus.autoCreateColumns" value="true"/>
 * 			
 * 		<property name="com.spidertracks.cassandra.serializer" value="com.spidertracks.datanucleus.serialization.XStreamSerializer"/>
 * 		
 * 		<property name="com.spidertracks.cassandra.bytemapper" value="cassandraByteCoverters.properties"/>
 * 
 * 	</persistence-manager-factory>
 * 
 * </jdoconfig>
 * </pre>
 * 
 * Note that each converter must implement the @see
 * 
 * @author Todd Nine
 * 
 */
public class ByteConverterContext {

	private Map<Class<?>, ByteConverter> converters;

	private ByteConverter boolConverter;
	private ByteConverter charConverter;
	private ByteConverter shortConverter;
	private ByteConverter intConverter;
	private ByteConverter doubleConverter;
	private ByteConverter longConverter;
	private ByteConverter floatConverter;
	private ByteConverter stringConverter;
	private ByteConverter serializerConverter;

	private Serializer serializer;

	public ByteConverterContext(String propertiesFilePath, Serializer serializer) {
		this.serializer = serializer;
		initialize(propertiesFilePath);

	}

	/**
	 * Initialize
	 * 
	 * @param propertiesFilePath
	 */
	private void initialize(String propertiesFilePath) {
		converters = new HashMap<Class<?>, ByteConverter>();

		/**
		 * Load our defaults
		 */
		converters.put(Boolean.class, new BooleanConverter());
		converters.put(Character.class, new CharacterConverter());
		converters.put(Double.class, new DoubleConverter());
		converters.put(Float.class, new FloatConverter());
		converters.put(Integer.class, new IntegerConverter());
		converters.put(java.util.UUID.class, new LexicalUUIDConverter());
		converters.put(Long.class, new LongConverter());
		converters.put(Short.class, new ShortConverter());
		converters.put(String.class, new StringConverter());
		converters.put(com.eaio.uuid.UUID.class, new TimeUUIDConverter());
		converters.put(byte[].class, new ByteArrayConverter());

		// Load user defined props and override defaults if required
		if (propertiesFilePath != null) {

			InputStream propsFileSource = this.getClass().getClassLoader()
					.getResourceAsStream(propertiesFilePath);

			if (propsFileSource == null) {
				throw new NucleusDataStoreException(String.format(
						"Could not load properties file %s from classpath",
						propertiesFilePath));
			}

			Properties converterProps = new Properties();
			try {
				converterProps.load(propsFileSource);
			} catch (IOException e) {
				throw new NucleusDataStoreException(String.format(
						"Could not load properties file %s from classpath",
						propertiesFilePath), e);
			}

			for (Entry<Object, Object> converterDef : converterProps.entrySet()) {
				String classKey = converterDef.getKey().toString();
				String converterClassName = converterDef.getValue().toString();

				try {
					ByteConverter instance = (ByteConverter) Class.forName(
							converterClassName).newInstance();

					converters.put(Class.forName(classKey), instance);

				} catch (Exception e) {
					throw new NucleusDataStoreException(
							String.format(
									"Unable to instanciate converter for key class %s with converter class %s.  Please make a no arg constructor is present in the converter",
									classKey, converterClassName), e);
				}

			}
		}

		// wire our our pointers to our primitive times after potential override
		this.boolConverter = converters.get(Boolean.class);
		this.charConverter = converters.get(Character.class);
		this.doubleConverter = converters.get(Double.class);
		this.floatConverter = converters.get(Float.class);
		this.intConverter = converters.get(Integer.class);
		this.longConverter = converters.get(Long.class);
		this.shortConverter = converters.get(Short.class);
		this.stringConverter = converters.get(String.class);

		this.serializerConverter = new SerializerWrapperConverter(serializer);

	}

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
	public Bytes getRowKey(ObjectProvider op) {

		return getRowKey(op.getExecutionContext(), op.getObject());

	}

	/**
	 * Get the stringified key for the given execution context and object
	 * 
	 * @param ec
	 * @param object
	 * @return
	 */
	public Bytes getRowKey(ExecutionContext ec, Object object) {
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
	public Bytes getRowKeyForId(ExecutionContext ec, Object id) {

		Object objectId = null;

		if (id instanceof ObjectIdentity) {
			objectId = ((ObjectIdentity) id).getKey();

		} else if (id instanceof SingleFieldIdentity) {
			objectId = ((SingleFieldIdentity) id).getKeyAsObject();
		} else {
			objectId = id;
		}

		ByteConverter converter = determineConverter(objectId.getClass(),
				ec.getTypeManager());

		if (converter == serializerConverter) {
			throw new NucleusDataStoreException(
					String.format(
							"You cannot use the default serializer on a key.  See the %s interface to use custom keys",
							ByteAware.class.getName()));
		}

		// else just call the default tostring since it's a user defined key
		converters.put(objectId.getClass(), converter);

		return converter.getBytes(objectId);
	}

	/**
	 * Convert the value to bytes using the defined converters. Converts in the
	 * following order
	 * <ol>
	 * <li>ByteConverter from converter Mapping</li>
	 * <li>ByteAware implementation</li>
	 * <li>ObjectLongConverter which is cached</li>
	 * <li>ObjectStringConverter which is cached</li>
	 * <li>Serialization using the serializer which is cached</li>
	 * </ol>
	 * 
	 * @param value
	 * @param ec
	 * @return
	 */
	public Bytes convertToBytes(Object value, ExecutionContext ec) {
		ByteConverter converter = converters.get(value.getClass());

		if (converter != null) {
			return converter.getBytes(value);
		}

		converter = determineConverter(value.getClass(), ec.getTypeManager());

		converters.put(value.getClass(), converter);

		return converter.getBytes(value);
	}

	/**
	 * Convert the bytes to a value using the defined converters.
	 * 
	 * <ol>
	 * <li>ByteConverter from converter Mapping</li>
	 * <li>ByteAware implementation</li>
	 * <li>ObjectLongConverter which is cached</li>
	 * <li>ObjectStringConverter which is cached</li>
	 * <li>Serialization using the serializer which is cached</li>
	 * </ol>
	 * 
	 * @param value
	 * @param ec
	 * @return
	 */
	public Object convertToObject(Bytes value, ExecutionContext ec,
			AbstractClassMetaData meta, int fieldNumber) {

		AbstractMemberMetaData member = meta
				.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

		ByteConverter converter = converters.get(member.getType());

		if (converter != null) {
			return converter.getObject(value);
		}

		converter = determineConverter(value.getClass(), ec.getTypeManager());

		converters.put(value.getClass(), converter);

		return converter.getObject(value);

	}

	/**
	 * Determine the converter that should be used for this class. Will not
	 * perform any caching on converters that are created, however it does check
	 * the cache for existing definitions Creates converters with the following
	 * order
	 * 
	 * <ol>
	 * <li>ByteConverter from converter Mapping</li>
	 * <li>ByteAware implementation</li>
	 * <li>ObjectLongConverter which is cached</li>
	 * <li>ObjectStringConverter which is cached</li>
	 * <li>Serialization using the serializer which is cached</li>
	 * </ol>
	 * 
	 * @param clazz
	 * @return
	 */
	private ByteConverter determineConverter(Class<?> clazz,
			TypeManager typeManager) {

		ByteConverter converter = converters.get(clazz);

		if (converter != null) {
			return converter;
		}

		if (ByteAware.class.isAssignableFrom(clazz)) {
			return new ByteAwareConverter(clazz);
		}

		ObjectLongConverter dnLongConverter = typeManager
				.getLongConverter(clazz);

		if (dnLongConverter != null) {
			return new ObjectLongWrapperConverter(dnLongConverter,
					this.longConverter);
		}

		ObjectStringConverter dnStringConverter = typeManager
				.getStringConverter(clazz);

		if (dnStringConverter != null) {
			return new ObjectStringWrapperConverter(dnStringConverter,
					this.stringConverter);
		}

		return serializerConverter;

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
	public Object getObjectIdentity(ExecutionContext ec,
			Class<?> candidateClass, Bytes value) {

		ApiAdapter adapter = ec.getApiAdapter();

		AbstractClassMetaData cmd = ec.getMetaDataManager()
				.getMetaDataForClass(candidateClass,
						ec.getClassLoaderResolver());

		if (!adapter.isSingleFieldIdentityClass(cmd.getObjectidClass())) {
			throw new DatastoreFieldDefinitionException(
					"multiple field identity creation is currently unsupported in indexing");
		}
		// assume only one identity field

		try {
			// if the class of the pk is a primitive we'll want to get the value
			// then set it as a string

			Object targetId = convertToObject(value, ec, cmd,
					cmd.getPKMemberPositions()[0]);

			return ec.newObjectId(candidateClass, targetId);

		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Unable to serialize bytes to object identity.  Please make sure it has the same SerializationId, long or string converter is was stored with. ");
		}

	}

	/**
	 * Return the cassandra validator for the class specified. Using the
	 * converter to determine the type. See determineConverter to view the rules
	 * for creating a converter.
	 * 
	 * @param fieldClass
	 * @return
	 * @see determineConverter
	 */
	public String getValidationClass(Class<?> fieldClass, TypeManager manager) {

		ByteConverter converter = determineConverter(fieldClass, manager);

		return converter.getComparatorType();
	}

	public ByteConverter getBooleanConverter() {
		return this.boolConverter;
	}

	public ByteConverter getShortConverter() {
		return this.shortConverter;
	}

	public ByteConverter getIntConverter() {
		return this.intConverter;
	}

	public ByteConverter getDoubleConverter() {
		return this.doubleConverter;
	}

	public ByteConverter getLongConverter() {
		return this.longConverter;
	}

	public ByteConverter getFloadConverter() {
		return this.floatConverter;
	}

	public ByteConverter getStringConverter() {
		return this.stringConverter;
	}

	public ByteConverter getCharConverter() {
		return charConverter;
	}

}
