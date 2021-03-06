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
package com.spidertracks.datanucleus.collection;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * Object for writing collection columns
 * 
 * @author Todd Nine
 * 
 */
public class ReadMap extends ExternalEntity implements Iterable<CassEntry>,
		Iterator<CassEntry> {

	private Class<?> keyClass;

	private Class<?> valueClass;

	private int index = -1;

	private List<Column> columns;

	public ReadMap(ByteConverterContext context, String ownerColumnFamily,
			Bytes rowKey, Bytes ownerColumn, Class<?> keyClass,
			Class<?> valueClass) {
		super(context, ownerColumnFamily, rowKey, ownerColumn);
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	/**
	 * Get the slice predicate for selecting all the columns from the given
	 * start key (inclusive). If the key is null, the range reads from the
	 * beginning. Reads up to the max for the given column from start using
	 * count.
	 * 
	 * @param count
	 * @param startKey
	 * @return
	 */
	public void fetchColumns(int count, Bytes startKey, Selector selector) {

		SliceRange range = new SliceRange();

		int length = ownerColumn.length() + 1;

		if (startKey != null) {
			length += startKey.length();
		}

		ByteBuffer startBuff = ByteBuffer.allocate(length);
		startBuff.mark();
		startBuff.put(ownerColumn.toByteArray());
		startBuff.put(DELIM_MIN);

		if (startKey != null) {
			startBuff.put(startKey.toByteArray());
		}

		startBuff.reset();

		range.setStart(startBuff);

		ByteBuffer endBuff = ByteBuffer.allocate(ownerColumn.length() + 1);
		endBuff.mark();
		endBuff.put(ownerColumn.toByteArray());
		endBuff.put(DELIM_MAX);
		endBuff.reset();

		range.setFinish(endBuff);
		range.setCount(count);

		SlicePredicate predicate = new SlicePredicate();

		predicate.setSlice_range(range);

		columns = selector.getColumnsFromRow(ownerColumnFamily, rowKey,
				predicate, Consistency.get());

	}

	public int getColumnCount() {
		return columns == null ? 0 : columns.size();
	}

	@Override
	public Iterator<CassEntry> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return columns != null && index + 1 < columns.size();
	}

	/**
	 * Returns the bytes as an object identity
	 */
	@Override
	public CassEntry next() {
		if (++index > columns.size()) {
			throw new NoSuchElementException("No elements left");
		}

		Column col = columns.get(index);

		ByteBuffer buffer = col.name;
		buffer.position(buffer.position() + ownerColumn.length() + 1);

		Object key = context.getObject(buffer, keyClass);

		Object value = context.getObject(col.value, valueClass);

		return new CassEntry(key, value);

	}

	@Override
	public void remove() {

		if (index == -1) {
			throw new IllegalStateException("next has not been called");
		}

		if (!hasNext()) {
			throw new IllegalStateException("No keys are left to parse");
		}

		columns.remove(index);
		// decrement our index so we read the next element when next is called
		index--;
	}

}
