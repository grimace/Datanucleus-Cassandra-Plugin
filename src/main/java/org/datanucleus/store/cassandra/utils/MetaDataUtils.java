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
package org.datanucleus.store.cassandra.utils;

import javax.jdo.identity.ObjectIdentity;
import javax.jdo.identity.SingleFieldIdentity;
import javax.jdo.spi.PersistenceCapable;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.mapped.exceptions.DatastoreFieldDefinitionException;
import org.datanucleus.store.types.ObjectStringConverter;

/**
 * Get the string key for the object associated with this state manager
 * 
 * @author Todd Nine
 * 
 */
public class MetaDataUtils {

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
	public static String getKey(StateManager stateManager) {

		return getKey(stateManager.getObjectProvider().getExecutionContext(),
				stateManager.getObject());

	}

	/**
	 * Get the stringified key for the given execution context and object
	 * 
	 * @param ec
	 * @param object
	 * @return
	 */
	public static String getKey(ExecutionContext ec, Object object) {
		Object id = ec.getApiAdapter().getIdForObject(object);

		if (id instanceof ObjectIdentity) {
			ObjectIdentity identity = (ObjectIdentity) id;

			ObjectStringConverter converter = ec.getTypeManager().getStringConverter(
					identity.getKey().getClass());
			
			if(converter == null){
				throw new DatastoreFieldDefinitionException(String.format("You must define an ObjectStringConverter for type %s", identity.getKey().getClass()));
			}
			
			return converter.toString(identity.getKey());
		} else if (id instanceof SingleFieldIdentity){
			
			SingleFieldIdentity identity = (SingleFieldIdentity) id;

			ObjectStringConverter converter = ec.getTypeManager().getStringConverter(
					identity.getKeyAsObject().getClass());
			
			if(converter != null){
				return converter.toString(identity.getKeyAsObject());
			}
			
			
			
			
		}

		// else just call the default tostring
		return id.toString();
	}

	/**
	 * Object for the identity in the AbstractClassMetaData's class.  Try and build it from the string
	 * 
	 * @param ec
	 * @param object
	 * @return
	 */
	public static Object getKeyValue(StateManager sm, AbstractClassMetaData cmd, String value) {
		
		ExecutionContext ec = sm.getObjectProvider().getExecutionContext();
	
	
		ClassLoaderResolver clr = ec.getClassLoaderResolver();
		
		
		//if we get here, we can assume it's persistenceCapable
		Object id  = ec.getApiAdapter().getObjectId(sm);
		
		ObjectStringConverter converter = null;
		
		if (id instanceof ObjectIdentity) {
			ObjectIdentity identity = (ObjectIdentity) id;

			converter = ec.getTypeManager().getStringConverter(
					identity.getKey().getClass());
			
			if(converter == null){
				throw new DatastoreFieldDefinitionException(String.format("You must define an ObjectStringConverter for type %s", identity.getKey().getClass()));
			}
			
			return converter.toObject(value);
		} else if (id instanceof SingleFieldIdentity){
			
			SingleFieldIdentity identity = (SingleFieldIdentity) id;

			converter = ec.getTypeManager().getStringConverter(
					identity.getTargetClass());
			
			if(converter != null){
				return converter.toObject(value);
			}
			
		}
		
		
		return value;
			

	}
	
}
