package com.alibaba.middleware.race.file;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU容器实现
 */
public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	
	private static final long serialVersionUID = -2960999970549803724L;  
	  
    private final int maxCapacity;  
  
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;  
  
    public LRULinkedHashMap(int maxCapacity) {  
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);  
        this.maxCapacity = maxCapacity;  
    }  
  
    @Override  
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {  
        return size() > maxCapacity;  
    }  
  

}
