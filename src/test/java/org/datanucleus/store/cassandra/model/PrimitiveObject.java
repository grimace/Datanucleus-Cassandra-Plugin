/**
 * 
 */
package org.datanucleus.store.cassandra.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;



/**
 * @author Todd Nine
 * Basic object for testing primitive persistence.  Only support types in java.lang
 * 
 */
@PersistenceCapable(table = "PrimitiveObject", identityType = IdentityType.APPLICATION)
public class PrimitiveObject {

	@PrimaryKey
	@Persistent(customValueStrategy = "uuid-cassandra")
	private String id;

	@Persistent
	private boolean testBool;

	@Persistent
	private short testShort;

	@Persistent
	private int testInt;

	@Persistent
	private long testLong;

	@Persistent
	private double testDouble;

	@Persistent
	private float testFloat;

	@Persistent
	private byte testByte;

	@Persistent
	private char testChar;

	@Persistent
	private String testString;

	public boolean isTestBool() {
		return testBool;
	}

	public void setTestBool(boolean testBool) {
		this.testBool = testBool;
	}

	public short getTestShort() {
		return testShort;
	}

	public void setTestShort(short testShort) {
		this.testShort = testShort;
	}

	public int getTestInt() {
		return testInt;
	}

	public void setTestInt(int testInt) {
		this.testInt = testInt;
	}

	public long getTestLong() {
		return testLong;
	}

	public void setTestLong(long testLong) {
		this.testLong = testLong;
	}

	public double getTestDouble() {
		return testDouble;
	}

	public void setTestDouble(double testDouble) {
		this.testDouble = testDouble;
	}

	public float getTestFloat() {
		return testFloat;
	}

	public void setTestFloat(float testFloat) {
		this.testFloat = testFloat;
	}

	public byte getTestByte() {
		return testByte;
	}

	public void setTestByte(byte testByte) {
		this.testByte = testByte;
	}

	public char getTestChar() {
		return testChar;
	}

	public void setTestChar(char testChar) {
		this.testChar = testChar;
	}

	public String getTestString() {
		return testString;
	}

	public void setTestString(String testString) {
		this.testString = testString;
	}

	public String getId() {
		return id;
	}

}
