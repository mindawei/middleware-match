package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashSet;
import java.util.Set;


public final class DefaultKeyValue implements KeyValue {
	
	String[] keys;
	byte[][] vals;
	byte num = 0;
	
	public DefaultKeyValue(){
		this(4);
	}
	
	public DefaultKeyValue(int cap){
		keys = new String[cap];
		vals = new byte[cap][];
	}

	@Override
	public KeyValue put(final String k,final int v) {
		keys[num] = k;
		vals[num] = new byte[] {
				(byte) ((v >> 24) & 0xff),
				(byte) ((v >> 16) & 0xff), 
				(byte) ((v >> 8)  & 0xff),
				(byte) ( v        & 0xff) 
			};
		num++;
		return this;
	}

	@Override
	public KeyValue put(final String k,final long v) {
		keys[num] = k;
		vals[num] = new byte[] { 
						(byte) ((v >> 56) & 0xff), 
						(byte) ((v >> 48) & 0xff),
						(byte) ((v >> 40) & 0xff),
						(byte) ((v >> 32) & 0xff),
						(byte) ((v >> 24) & 0xff), 
						(byte) ((v >> 16) & 0xff),
						(byte) ((v >> 8)  & 0xff), 
						(byte) ( v        & 0xff) 
				};
		num++;
		return this;
	}

	@Override
	public KeyValue put(final String k,final double d) {
		final long v = Double.doubleToRawLongBits(d);
		keys[num] = k;
		vals[num] = new byte[] { 
						(byte) ((v >> 56) & 0xff), 
						(byte) ((v >> 48) & 0xff),
						(byte) ((v >> 40) & 0xff),
						(byte) ((v >> 32) & 0xff),
						(byte) ((v >> 24) & 0xff), 
						(byte) ((v >> 16) & 0xff),
						(byte) ((v >> 8)  & 0xff), 
						(byte) ( v        & 0xff) 
				};
		num++;
		return this;
	}

	@Override
	public KeyValue put(final String k,final String v) {
		keys[num] = k;
		vals[num] = v.getBytes();
		num++;
		return this;
	}
	
	private final byte[] get(final String k) {
		for(byte i=0;i<num;++i){
			if(keys[i].equals(k)){
				return vals[i];
			}
		}
		return null;
	}

	final void put(final String k,final byte[] v) {
		keys[num] = k;
		vals[num] = v;
		num++;
	}

	@Override
	public int getInt(final String key) {
		final byte[] b = get(key);
		return ( 
				((b[0]       ) << 24) |
		        ((b[1] & 0xff) << 16) |
		        ((b[2] & 0xff) <<  8) |
		        ((b[3] & 0xff))
		       );
	}

	
	@Override
	public long getLong(final String k) {
		final byte[] b = get(k);
		return (
				(((long) b[0])        << 56) | 
				(((long) b[1] & 0xff) << 48) | 
				(((long) b[2] & 0xff) << 40) | 
				(((long) b[3] & 0xff) << 32) | 
				(((long) b[4] & 0xff) << 24) | 
				(((long) b[5] & 0xff) << 16) | 
				(((long) b[6] & 0xff) << 8 ) | 
				(((long) b[7] & 0xff)      )
			   );
	}

	@Override
	public double getDouble(final String k) {
		return Double.longBitsToDouble(getLong(k));
	}

	@Override
	public String getString(final String k) {
		final byte[] b = get(k); 
		return b == null ? null : new String(b);
	}

	private Set<String> keySet;
	
	@Override
	public Set<String> keySet() {
		if (keySet == null) {
			keySet = new HashSet<>(num);
			for (byte i = 0; i < num; ++i) {
				keySet.add(keys[i]);
			}
		}
		return keySet;
	}

	@Override
	public boolean containsKey(final String k) {
		if (keySet == null) {
			for (byte i = 0; i < num; ++i) {
				if (keys[i].equals(k)) {
					return true;
				}
			}
			return false;
		} else {
			return keySet.contains(k);
		}
	}

}
