package com.alibaba.middleware.race.sync;

public final class IntLongHashMap{

	/** The maximum number of elements allowed without allocating more space. */
	private int maxSize;

	// fixed 0.5
	// private final float loadFactor;

	private int[] keys;
	private long[] values;
	private int size;
	private int mask;
	
	public IntLongHashMap(int capacity) {

		mask = capacity - 1;

		// Allocate the arrays.
		keys = new int[capacity];
		values =  new long[capacity];

		// Initialize the maximum size value.
		maxSize = calcMaxSize(capacity);
	}

	public final long get(final int key) {
		int index = key & mask;
		for (;;) {
			if (key == keys[index]) {
				return values[index];
			}
			index = ((index + 1) & mask);
		}
	}

	public final void put(final int key,final long value) {
		int index = key & mask;
		for (;;) {
			if (values[index] == NULL) {
				keys[index] = key;
				values[index] = value;
				
				size++;
				if (size > maxSize) {
					// Double the capacity.
					rehash(keys.length << 1);
				}
				return;
			}
			
			if (keys[index] == key) {
				values[index] = value;
				return;
			}
			index = ((index + 1) & mask);
		}
	}

	private static final long NULL = 0;

	/**
	 * Calculates the maximum size allowed before rehashing.
	 */
	private int calcMaxSize(final int capacity) {
		return Math.min(capacity - 1, (capacity >>>1));
	}

	/**
	 * Rehashes the map for the given capacity.
	 *
	 * @param newCapacity
	 *            the new capacity for the map.
	 */
	private void rehash(int newCapacity) {
		int[] oldKeys = keys;
		long[] oldVals = values;

		keys = new int[newCapacity];
		long[] temp = new long[newCapacity];
		values = temp;

		maxSize = calcMaxSize(newCapacity);
		mask = newCapacity - 1;

		// Insert to the new arrays.
		for (int i = 0; i < oldVals.length; ++i) {
			long oldVal = oldVals[i];
			if (oldVal != NULL) {
				// Inlined put(), but much simpler: we don't need to worry about
				// duplicated keys, growing/rehashing, or failing to insert.
				int oldKey = oldKeys[i];
				int index = oldKey & mask;

				for (;;) {
					if (values[index] == NULL) {
						keys[index] = oldKey;
						values[index] = oldVal;
						break;
					}

					// Conflict, keep probing. Can wrap around, but never
					// reaches startIndex again.
					index = ((index + 1) & mask);
				}
			}
		}
	}
    
}