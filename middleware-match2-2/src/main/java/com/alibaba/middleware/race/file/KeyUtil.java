package com.alibaba.middleware.race.file;

import java.util.HashSet;
import java.util.Set;

public class KeyUtil {
	
	private static KeyUtil instance = new KeyUtil();
	
	private KeyUtil(){}
	
	public static KeyUtil getInstance(){
		return instance;
	}

	// >> 字段id
	
	public static final String ORDER_ID = "orderid";


	public static final String BUYER_ID = "buyerid";
    
  
	public static final String GOOD_ID  = "goodid";
    
    
	public static final String CREATE_TIME = "createtime";
    
   
	public static final String SALER_ID = "salerid";
	
	/**
	 * 所有可查询的key,文件全部处理完后添加
	 */
	public final Set<String> allKeys = new HashSet<String>();
	
	/**
	 * 订单keys
	 */
	public final Set<String> orderKeys = new HashSet<String>();
	
	/**
	 * 买家keys
	 */
	public final Set<String> buyerKeys = new HashSet<String>();
	
	/**
	 * 买家keys
	 */
	public final Set<String> goodKeys = new HashSet<String>();
	
	
	/** 初始化 */
	public void init(Set<String> orderKeys, Set<String> buyerKeys,Set<String> goodKeys){
		
		this.orderKeys.addAll(orderKeys);
		this.allKeys.addAll(orderKeys);
		
		this.buyerKeys.addAll(buyerKeys);
		this.allKeys.addAll(buyerKeys);
		
		this.goodKeys.addAll(goodKeys);
		this.allKeys.addAll(goodKeys);
	
	}
	
	
}
