package com.alibaba.middleware.race;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;

/**
 * 结果实现类
 */
public class ResultImpl implements Result {
	
	private long orderid = 0;
	private Map<String,KV> kvMap;

	private ResultImpl(long orderid,Map<String,KV> kvMap) {
		this.orderid = orderid;
		this.kvMap = kvMap;
	}

	/**
	 * 静态构建函数(创建工厂)
	 * @param needQueryingKeys 需要创建的keys。如果为空，全部忽略；确保不为null。
	 */
	public static Result createResult(long orderid,Map<String,String> allData, Collection<String> needQueryingKeys) {
		
		// 创建  kvMap
		Map<String,KV> kvMap = new HashMap<String, KV>();
		
		for (Map.Entry<String, String> entry: allData.entrySet()) {
			String entryKey = entry.getKey();
			if (needQueryingKeys.contains(entryKey)) {
				kvMap.put(entryKey, new KV(entryKey, entry.getValue()));
			}
		}
		
		// 返回Result
		return new ResultImpl(orderid,kvMap);
	}
	
	@Override
	public KeyValue get(String key) {
		return this.kvMap.get(key);
	}

	@Override
	public KeyValue[] getAll() {
		return kvMap.values().toArray(new KeyValue[0]);
	}

	@Override
	public long orderId() {
		return orderid;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("orderid: " + orderid + " {");
		if (kvMap != null && !kvMap.isEmpty()) {
			for (KV kv : kvMap.values()) {
				sb.append(kv.toString());
				sb.append(",\n");
			}
		}
		sb.append('}');
		return sb.toString();
	}
	
}
