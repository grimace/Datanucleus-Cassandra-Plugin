package com.spidertracks.datanucleus.serialization;

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

@Ignore("Waiting on response from mailing list before finishing")
public class JacksonSerializerTest {

	@Test
	public void testConvert() {
		JacksonSerializer serializer = new JacksonSerializer();
		
		Pack pack = new Pack();

		Card aceSpades = new Card();
		aceSpades.setName("Ace of Spades");
		pack.addCard(aceSpades);
		aceSpades.setPack(null);
		
		
		Card jackHearts = new Card();
		jackHearts.setName("Jack of Hearts");
		pack.addCard(jackHearts);
		jackHearts.setPack(null);
		
		
		
		byte[] bytes = serializer.getBytes(pack);
		
		Pack returned = serializer.getObject(bytes);
		
		assertEquals(pack, returned);
		
		assertEquals(pack.getCards().get(0), aceSpades);
		assertEquals(pack.getCards().get(1), jackHearts);
	}

	

}
