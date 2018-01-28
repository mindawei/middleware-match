package com.alibaba.middleware.race.file;

/**
 * 买家订单信息
 */
public class BuyerOrderInfo {
	
	long orderId;
	long createtime;
	
	public BuyerOrderInfo(long orderId, long createtime) {
		super();
		this.orderId = orderId;
		this.createtime = createtime;
	}
	
}
