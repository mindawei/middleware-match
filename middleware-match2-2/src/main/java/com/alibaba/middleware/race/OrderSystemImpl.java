package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.*;
import com.alibaba.middleware.race.file.*;

/**
 * 实现交易订单系统接口
 */
public class OrderSystemImpl implements OrderSystem {
		
	private final Query query = new Query();
	private final KeyUtil keyUtil = KeyUtil.getInstance();
	private final Statistic statistic = Statistic.getInstance();
	private final DataReader reader = DataReader.getInstance();
	private final List<Result> EMPTY_RESULT_LIST = new ArrayList<Result>(0);
	
	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles,
			Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		
		Constructor.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

	}
	
	private final long sleepTime = 5000; // 5s
	
	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		
		while(!Constructor.isFinished){
			try {
				Thread.sleep(sleepTime); 
			} catch (InterruptedException e) {
			} 
		}	
		
		// 查询所有的字段
		if (keys == null)
			keys = keyUtil.allKeys;
		
		// 划分 keys
		Map<String, String> allData = new HashMap<String,String>();
		
		List<String> orderKeys = new ArrayList<String>();
		List<String> buyerKeys = new ArrayList<String>();
		List<String> goodKeys = new ArrayList<String>();
		
		for(String key : keys){
			if(keyUtil.orderKeys.contains(key)){
				if(key==KeyUtil.ORDER_ID){
					// 可以提供
					allData.put(KeyUtil.ORDER_ID, Long.toString(orderId));
				}else{
					orderKeys.add(key);	
				}
			}else if(keyUtil.buyerKeys.contains(key)){
				buyerKeys.add(key);
			}else if(keyUtil.goodKeys.contains(key)){
				goodKeys.add(key);
			}
		}
		
		Map<String, String> orderData = null;
		Map<String, String> buyerData = null;
		Map<String, String> goodData = null;
		
		boolean orderExist = false;
				
		if(orderKeys.size()>0){ // 需要查order
			orderData = query.queryOrderData(orderId);
			if(orderData!=null)
				orderExist = true;
		}else{ // 需要判断order是否存在
			orderExist = reader.isOrderIdExist(orderId);
		}
		
		// 订单不存在
		if(!orderExist)
			return null;
		
		// 订单存在且还要查其它的
		if(orderExist&&(buyerKeys.size()>0||goodKeys.size()>0)){
			
			String buyerId;
			String goodId;
			if(orderData!=null){
				buyerId = orderData.get(KeyUtil.BUYER_ID);
				goodId = orderData.get(KeyUtil.GOOD_ID);
			}else{
				/// buyerId,goodId
				List<String> orderInfo = reader.readOrderInfo(orderId);
				buyerId = orderInfo.get(0);
				goodId = orderInfo.get(1);
			}
			
			if(buyerKeys.size()>0){
				buyerData = query.queryBuyerData(buyerId);
			}
			
			if(goodKeys.size()>0){
				goodData = query.queryGoodData(goodId);
			}

		}
		
		// 补充数据
		for(String key : orderKeys){
			if(orderData.containsKey(key))
				allData.put(key, orderData.get(key));
		}
		
		for(String key : buyerKeys){
			if(buyerData.containsKey(key))
				allData.put(key, buyerData.get(key));
		}
		
		for(String key : goodKeys){
			if(goodData.containsKey(key))
				allData.put(key, goodData.get(key));
		}
	
		// 订单存在
		return ResultImpl.createResult(orderId, allData, keys);

	}

	 
	/// >>> 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
	
	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,String buyerid) {
		
		while(!Constructor.isFinished){
			try {
				Thread.sleep(10000); // 10s
			} catch (InterruptedException e) {
			} 
		}
		
		// 排除时间范围外的
		if(endTime<statistic.orderStartTime
				||startTime>statistic.orderEndTime){
			return new NullIterator();
		}
		
		/// >>> 先查找买家符合时间的订单
		List<Long> orderids = query.queryOrderIdsOfBuyer(buyerid,startTime,endTime);
		
		// 没有时间范围内的订单
		int orderIdsSize = orderids.size();
		if(orderIdsSize==0){
			return new NullIterator();
		}
		
		// 需要返回的结果
		List<Result> orders = new ArrayList<Result>(orderIdsSize);
			
		// 找这些订单的全量信息
		Map<Long, Map<String, String>> rowMaps = query.queryOrdersDataOfBuyer(orderids);
				
		for(Map.Entry<Long,  Map<String, String>> entry : rowMaps.entrySet()){
			Long orderid = entry.getKey();
			Map<String, String> rowMap = entry.getValue();
			orders.add(ResultImpl.createResult(orderid,rowMap, keyUtil.allKeys));
		}
		
		// 按时间戳从大到小排序
		Collections.sort(orders, new Comparator<Result>(){
			@Override
			public int compare(Result r1, Result r2) {
			
				try {
					long t1 = r1.get("createtime").valueAsLong();
					long t2 = r2.get("createtime").valueAsLong();
					if(t1<t2)
						return 1;
					else if(t1>t2)
						return -1;
					else
						return 0;
				} catch (TypeException e) {
					return 0;
				}
			}
			
		});
		
		return new ResultIterator(orders);
		
	}
	
	
	/// >>> 查询某位卖家某件商品所有订单的某些字段
	
	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,Collection<String> keys) {
		
		while(!Constructor.isFinished){
			try {
				Thread.sleep(sleepTime); 
			} catch (InterruptedException e) {
			} 
		}
		
		// 查找商品的订单
		List<Long> orderids = query.queryOrderIdsOfGood(goodid);
		int orderIdsSize = orderids.size();
	
		// 该商品没有订单
		if(orderIdsSize==0){
			return new ResultIterator(EMPTY_RESULT_LIST);
		}
			
 		// 查全部
 		if(keys==null)
			keys = keyUtil.allKeys;
 		
 		// 需要返回的结果
	    List<Result> orders = new ArrayList<Result>();
		
	    // 单个调用
		for(Long orderId :orderids){
			Result result = queryOrder(orderId, keys);
			orders.add(result);
		}
		
		// 按照订单id从小至大排序
		Collections.sort(orders, new Comparator<Result>() {
			@Override
			public int compare(Result r1, Result r2) {
				if(r1.orderId()<r2.orderId())
					return -1;
				else if(r1.orderId()>r2.orderId())
					return 1;
				else
					return 0;
			}
		});
		
		return new ResultIterator(orders);

	}
	
	
	
	/// >>> 对某件商品的某个字段求和	
	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		
		while(!Constructor.isFinished){
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			} 
		}
		
		// 查询的key不存在
		if(!keyUtil.allKeys.contains(key)){
			return null;
		}
			
		// 查找商品的订单
		List<Long> orderids =  query.queryOrderIdsOfGood(goodid);
		int orderIdsSize = orderids.size();
			
		// 该商品没有订单
		if(orderIdsSize==0){
			return null;
		}
		
		// 查某一个 key
		Collection<String> keys = Arrays.asList(key);		
			
		// 需要返回的结果
		List<Result> orders = new ArrayList<Result>();
				
	    // 单个调用
		for(Long orderId :orderids){
			Result result = queryOrder(orderId, keys);
			orders.add(result);
		}

		// accumulate as Long	
		try {
			boolean hasValidData = false;
			long sum = 0;
			for (Result result : orders) {
				KeyValue kv = result.get(key);
				if (kv != null) {
					sum += kv.valueAsLong();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				KeyValue result = new KV(key, Long.toString(sum));
				return result;
			}
		} catch (TypeException e) {
		}

		// accumulate as double
		try {
			boolean hasValidData = false;
			double sum = 0;
			for (Result result : orders) {
				KeyValue kv = result.get(key);
				if (kv != null) {
					sum += kv.valueAsDouble();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				KeyValue result = new KV(key, Double.toString(sum));
				return result;
			}
		} catch (TypeException e) {}

		return null;
	}
	
	/**
	 * 有结果
	 */
	class ResultIterator implements Iterator<Result>{

		private List<Result> items;
		private int currentIndex = -1;
		
		public ResultIterator(List<Result> orders) {
			super();
			this.items = orders;
		}

		@Override
		public boolean hasNext() {
			return (currentIndex+1)<items.size();
		}

		@Override
		public Result next() {
			currentIndex++;
			return items.get(currentIndex);
		}

		@Override
		public void remove() {}

	}
	

	/**
	 * 空结果
	 */
	class NullIterator implements Iterator<Result>{
		
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Result next() {
			return null;
		}

		@Override
		public void remove() {
		}
	}
	
}
