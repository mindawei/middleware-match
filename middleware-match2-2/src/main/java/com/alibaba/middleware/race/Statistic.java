package com.alibaba.middleware.race;

public class Statistic {
	
	private static Statistic instance = new Statistic();
	
	private Statistic(){}
	
	public static Statistic getInstance(){
		return instance;
	}
	
	/** 订单开始时间 */
	public long orderStartTime = 1468385553L;
	/** 订单结束时间 */
	public long orderEndTime = 11668409867L;
	
	/**
	 * 更新时间
	 */
//	public synchronized void updateTime(long time){
//		if(time<orderStartTime){
//			orderStartTime = time;
//		}
//		if(time>orderEndTime){
//			orderEndTime = time;
//		}
//	}
	
	
//	/** 订单数目 */
//	public AtomicLong orderNum = new AtomicLong();
//	/** 买家数目 */
//	public AtomicLong buyerNum = new AtomicLong();
//	/** 商品数目 */
//	public AtomicLong goodNum = new AtomicLong();
	
}
