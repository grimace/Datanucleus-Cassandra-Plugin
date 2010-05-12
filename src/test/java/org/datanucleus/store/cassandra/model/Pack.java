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
package org.datanucleus.store.cassandra.model;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * An object with a collection to many objects
 * @author Todd Nine 
 */
@PersistenceCapable(table = "Pack", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class Pack extends BaseEntity {
	
	
	@Persistent(mappedBy="pack")
	private List<Card> cards;
	
	public Pack(){
		cards = new ArrayList<Card>();
	}

	/**
	 * @return the manyToOne
	 */
	public List<Card> getCards() {
		return cards;
	}
	
	public void AddCard(Card card){
		this.cards.add(card);
		card.setPack(this);
	}

	
}
