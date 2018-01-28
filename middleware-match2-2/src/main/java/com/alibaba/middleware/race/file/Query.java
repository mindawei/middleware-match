package com.alibaba.middleware.race.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 查询类
 */
public class Query {
	
	private final int CAHCE_MAX_CAPACITY = 700000;
	
	private final FileMapper fileMapper = FileMapper.getInstance();
	
	private final DataReader dataReader = DataReader.getInstance();
	
	/// >>> 订单查询相关
	
	private final LRULinkedHashMap<Long, Map<String, String>> orderDataCahce = new LRULinkedHashMap<Long, Map<String, String>>(CAHCE_MAX_CAPACITY);
	
	private final Lock orderDataCahceLock = new ReentrantLock();  
	  	
	/**
	 * 在指定文件中查找指定订单id的信息,返回结果不能修改
	 * 
	 * @param orderId 需要查找的订单id
	 * @return 存在返回Map,不存在返回null
	 */
	public Map<String, String> queryOrderData(long orderId) {
	
			// 先查看缓存
			orderDataCahceLock.lock();
			if (orderDataCahce.containsKey(orderId)) {
				Map<String, String> cacheData = orderDataCahce.get(orderId);
				orderDataCahceLock.unlock();
				return cacheData;
			}
			orderDataCahceLock.unlock();
	
			String indexFileName = fileMapper.getOrderDataIndexPath(orderId);
			Map<String, String> rowMap = dataReader.readData(indexFileName,String.valueOf(orderId));

			// 正常查询
			orderDataCahceLock.lock();
			orderDataCahce.put(orderId, rowMap);
			orderDataCahceLock.unlock();
			
			return rowMap;

	}
	
	
	static class SearchInfo {
		long orderId;
		String indexfileName;
		String fileName;
		long pos;
		int len;
		Map<String, String> kvMap;
		@Override
		public String toString() {
			return "SearchInfo [orderId=" + orderId + ", indexfileName="
					+ indexfileName + ", fileName=" + fileName + ", pos=" + pos
					+ ", len=" + len + ", kvMap=" + kvMap + "]";
		}	
	}
	
	/**
	 * 批量操作，返回结果不能修改，修改需要再创建对象进行拷贝
	 */
	private Map<Long, Map<String, String>> queryOrderDatas(List<Long> orderIds) {
		
		// 需要返回的结果 orderid -> map
		Map<Long, Map<String, String>> result = new HashMap<Long, Map<String, String>>();

		// 要查询的订单数量
		int orderIdsSize = orderIds.size();

		// 先查看缓存
		orderDataCahceLock.lock();
		for (int i = 0; i < orderIdsSize; ++i) {
			Long orderId = orderIds.get(i);
			if (orderDataCahce.containsKey(orderId)) {
				result.put(orderId, orderDataCahce.get(orderId));
			}
		}
		orderDataCahceLock.unlock();

		// 全部找完
		if (result.size() == orderIdsSize) {
			return result;
		}
		
		List<SearchInfo> searchInfos = new ArrayList<SearchInfo>();
		
		// 1 orderId,indexFileName
		for (int i = 0; i < orderIdsSize; ++i) {
			
			Long orderId = orderIds.get(i);
			// 缓存已命中的不用找
			if (result.containsKey(orderId))
				continue;
			
			SearchInfo searchInfo = new SearchInfo();
			searchInfo.orderId = orderId;
			searchInfo.indexfileName = fileMapper.getOrderDataIndexPath(orderId);
			searchInfos.add(searchInfo);
		
		}
			
		// 2 按indexFileName划分
		Map<String,List<SearchInfo>> indexfileNameMap = new HashMap<>();
		
		for(SearchInfo searchInfo :searchInfos){
			String indexfileName = searchInfo.indexfileName;
			if(indexfileNameMap.containsKey(indexfileName)){
				indexfileNameMap.get(indexfileName).add(searchInfo);
			}else{
				List<SearchInfo> infos = new ArrayList<>();
				infos.add(searchInfo);
				indexfileNameMap.put(indexfileName, infos);
			}
		}	
		
		// 3 补充 fileName,pos,len
		for(Map.Entry<String,List<SearchInfo>> indexfileNameMapEntry : indexfileNameMap.entrySet()){
			String indexFileName = indexfileNameMapEntry.getKey();
			List<SearchInfo> infos =  indexfileNameMapEntry.getValue();
			dataReader.getFileNamePosLen(indexFileName,infos);
		}
		
		// 4 按 fileName 进行划分
		Map<String,List<SearchInfo>> fileNameMap = new HashMap<>();
		
		for(SearchInfo searchInfo :searchInfos){
			String fileName = searchInfo.fileName;
			if(fileNameMap.containsKey(fileName)){
				fileNameMap.get(fileName).add(searchInfo);
			}else{
				List<SearchInfo> infos = new ArrayList<>();
				infos.add(searchInfo);
				fileNameMap.put(fileName, infos);
			}
		}
		
		
		// / >> 开始寻找
		
		for(Map.Entry<String,List<SearchInfo>> fileNameMapEntry : fileNameMap.entrySet()){
			String fileName = fileNameMapEntry.getKey();
			List<SearchInfo> infos =  fileNameMapEntry.getValue();
			dataReader.getOrderDatas(fileName,infos);

			orderDataCahceLock.lock();
			for(SearchInfo info : infos){
				result.put(info.orderId, info.kvMap);
				orderDataCahce.put(info.orderId, info.kvMap);
			}
			orderDataCahceLock.unlock();
		}

		return result;
	}
	
	/**
	 * 查询某一买家的订单
	 */
	public Map<Long, Map<String, String>> queryOrdersDataOfBuyer(List<Long> orderIds) {

		// 查找订单(返回对象是新建对象,只有orderData需要新建，因为其他返回对象都不会修改)
		Map<Long, Map<String, String>> orderDatas = queryOrderDatas(orderIds);
	
		// 买家数据只要查一次
		Map<String, String> buyerData = null;
		
		// 要补充数据
		for (Map.Entry<Long, Map<String, String>> entry : orderDatas.entrySet()){
			
			Long orderId = entry.getKey();
			Map<String, String> orderData = entry.getValue();
			
			// 需要修改 orderMap时需要拷贝,防止并发出错
			Map<String, String> allData = new HashMap<String, String>();
			allData.putAll(orderData);
			
			// 补充数据
			Map<String, String> goodData = queryGoodData(orderData.get(KeyUtil.GOOD_ID));
			allData.putAll(goodData);
		
			// 补充数据
			if(buyerData==null)
				buyerData = queryBuyerData(orderData.get(KeyUtil.BUYER_ID));
			allData.putAll(buyerData);
			
			orderDatas.put(orderId, allData);

		}

		return orderDatas;
	}


	
	/// >>> 买家查询相关

	private final LRULinkedHashMap<String, Map<String, String>> buyerDataCahce = new LRULinkedHashMap<String, Map<String, String>>(CAHCE_MAX_CAPACITY);
	
	private final Lock buyerDataCahceLock = new ReentrantLock();  
	

	/**
	 * 在指定文件中查找指定买家id的信息
	 * 
	 * @param buyerid 需要查找的买家id
	 * @return 存在返回Map,不存在返回null
	 */
	public Map<String, String> queryBuyerData(String buyerid) {

			// 先查看缓存
			buyerDataCahceLock.lock();
			if (buyerDataCahce.containsKey(buyerid)) {
				Map<String, String> result = buyerDataCahce.get(buyerid);
				buyerDataCahceLock.unlock();
				return result;
			}
			buyerDataCahceLock.unlock();

			// 获得地址
			String indexFileName = fileMapper.getBuyerDataIndexPath(buyerid);
			Map<String, String> rowMap = dataReader.readData(indexFileName, buyerid);

			// 刷新缓存
			buyerDataCahceLock.lock();
			buyerDataCahce.put(buyerid, rowMap);
			buyerDataCahceLock.unlock();

			return rowMap;

	

	}

	/// >>> 商品查询相关

	private final LRULinkedHashMap<String, Map<String, String>> goodDataCahce = new LRULinkedHashMap<String, Map<String, String>>(CAHCE_MAX_CAPACITY);
	
	private final Lock goodDataCahceLock = new ReentrantLock();  
	

	/**
	 * 在指定文件中查找指定商品id的信息
	 * 
	 * @param goodid 需要查找的商品id
	 * @return 存在返回Map,不存在返回null
	 */
	public Map<String, String> queryGoodData(String goodid) {

			// 先查看缓存
			goodDataCahceLock.lock();
			if (goodDataCahce.containsKey(goodid)) {
				Map<String, String> result = goodDataCahce.get(goodid);
				goodDataCahceLock.unlock();
				return result;
			}
			goodDataCahceLock.unlock();

			// 获得地址
			String indexFileName = fileMapper.getGoodDataIndexPath(goodid);
			Map<String, String> rowMap = dataReader.readData(indexFileName, goodid);

			// 刷新缓存
			goodDataCahceLock.lock();
			goodDataCahce.put(goodid, rowMap);
			goodDataCahceLock.unlock();
			
			return rowMap;


	}

	/// >>> 买家信息相关
	
	private final LRULinkedHashMap<String, List<BuyerOrderInfo>> buyerInfoCahce = new LRULinkedHashMap<String, List<BuyerOrderInfo>>(CAHCE_MAX_CAPACITY);
	
	private final Lock buyerInfoCahceLock = new ReentrantLock();  

	
	/**
	 * 在指定文件中查找指定买家id对应的订单id
	 * 
	 * @param file 需要查找的文件
	 * @param buyerid 需要查找的买家id
	 * @return 返回orderid的列表
	 */
	public List<Long> queryOrderIdsOfBuyer(String buyerid,long startTime,long endTime) {
		
			// 先查看缓存
		    buyerInfoCahceLock.lock();
			if (buyerInfoCahce.containsKey(buyerid)) {
				List<BuyerOrderInfo> orderInfos = buyerInfoCahce.get(buyerid);
				List<Long> result = getOrderIds(orderInfos, startTime, endTime);
				buyerInfoCahceLock.unlock();
				return result;
			}
			buyerInfoCahceLock.unlock();

			List<BuyerOrderInfo> orderInfos = dataReader.readBuyerInfo(buyerid);
			
		
			// 刷新缓存
			buyerInfoCahceLock.lock();
			buyerInfoCahce.put(buyerid, orderInfos);
			buyerInfoCahceLock.unlock();
			
			return getOrderIds(orderInfos, startTime, endTime);

	}
	
	/**
	 * 从订单中找的合适的
	 */
	private List<Long> getOrderIds(List<BuyerOrderInfo> orderInfos ,long startTime,long endTime){
		List<Long> orderIds = new ArrayList<Long>();
		for(BuyerOrderInfo orderInfo : orderInfos){
			// 时间符合要求
			if(orderInfo.createtime>=startTime&&orderInfo.createtime<endTime){
				orderIds.add(orderInfo.orderId);
			}
		}
		return orderIds;
	}
	

	/// >>> 商品信息相关

	private final LRULinkedHashMap<String, List<Long>> goodInfoCahce = new LRULinkedHashMap<String, List<Long>>(CAHCE_MAX_CAPACITY);
	
	private final Lock goodInfoCahceLock = new ReentrantLock();  


	/**
	 * 在指定文件中查找指定商品id对应的订单id
	 * 
	 * @param file 需要查找的文件
	 * @param goodid 需要查找的商品id
	 * @return 返回orderid的列表
	 */
	public List<Long> queryOrderIdsOfGood(String goodid) {
		
			// 先查看缓存
			goodInfoCahceLock.lock();
			if (goodInfoCahce.containsKey(goodid)) {
				List<Long> result = goodInfoCahce.get(goodid);
				goodInfoCahceLock.unlock();
				return result;
			}
			goodInfoCahceLock.unlock();
			
			List<Long> orderIds = dataReader.readGoodInfo(goodid);
			
			// 刷新缓存
			goodInfoCahceLock.lock();
			goodInfoCahce.put(goodid, orderIds);
			goodInfoCahceLock.unlock();

			return orderIds;

	}

	

}
