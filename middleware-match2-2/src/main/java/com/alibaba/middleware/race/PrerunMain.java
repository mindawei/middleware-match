
package com.alibaba.middleware.race;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.file.FileMapper;

public class PrerunMain {

	/**
	 * http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/a44af8f9a81._latest.tar.xz 
	 * 日志地址：http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/baedd92146b.tar.xz
	 * 
	 * 试跑数据地址： http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/prerun_data.tar.gz
	 * 
	 */
	public static void main(String[] args) throws IOException, InterruptedException{
		String basePath = "H:/data/";
		//String basePath = "prerun_data/";
		//String basePath = "data/test/";
		FileMapper.getInstance().makeDirs(basePath);
		String basePathOut = basePath+"out/";
		//String basePathOut = "data/out/";
		FileMapper.getInstance().makeDirs(basePathOut);
		
		// get file names 
		List<String> orderFiles = new ArrayList<String>();
		for(File file: new File(basePath+"order_records").listFiles())
			orderFiles.add(file.getAbsolutePath());
		
		List<String> buyerFiles = new ArrayList<String>();
		for(File file: new File(basePath+"buyer_records").listFiles())
			buyerFiles.add(file.getAbsolutePath());
		
		List<String> goodFiles = new ArrayList<String>();
		for(File file: new File(basePath+"good_records").listFiles())
			goodFiles.add(file.getAbsolutePath());
		
		List<String> storeFolders = new ArrayList<String>();
		// storeFolders.add(basePathOut);
		storeFolders.add(basePathOut+"/disk1/");
		storeFolders.add(basePathOut+"/disk2/");
		storeFolders.add(basePathOut+"/disk3/");
//		
			
		
		// prepare data
		final OrderSystemImpl os = new OrderSystemImpl();
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
		
		
		System.exit(0);
		
		Iterator<Result> it2 = os.queryOrdersByBuyer(1465037309, 1475645184, "ap-ac90-ebd352c7b24d");
    	int index = 0;
    	while (it2.hasNext()) {
    		index++;
    		Result r = it2.next();
    		if(index==1)
			System.out.println("2:"+r.get("orderid"));
			
		}
		
		
		
		
//		boolean wait = true;
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				while(true){
//					Iterator<Result> it2 = os.queryOrdersByBuyer(1465037309, 1475645184, "ap-ac90-ebd352c7b24d");
//			    	int index = 0;
//			    	while (it2.hasNext()) {
//			    		index++;
//			    		Result r = it2.next();
//			    		if(index==1)
//						System.out.println("1:　"+r.get("orderid"));
//						
//					}
//			    	
//				}
//			}
//			
//		}).start();
//		
//		
//	new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				while(true){
//					Iterator<Result> it2 = os.queryOrdersByBuyer(1465037309, 1475645184, "ap-ac90-ebd352c7b24d");
//			    	int index = 0;
//			    	while (it2.hasNext()) {
//			    		index++;
//			    		Result r = it2.next();
//			    		if(index==1)
//						System.out.println("2:"+r.get("orderid"));
//						
//					}
//			    	
//				}
//			}
//			
//		}).start();
//	
//	
//	new Thread(new Runnable() {
//		
//		@Override
//		public void run() {
//			while(true){
//				Iterator<Result> it2 = os.queryOrdersByBuyer(1465037309, 1475645184, "ap-ac90-ebd352c7b24d");
//		    	int index = 0;
//		    	while (it2.hasNext()) {
//		    		index++;
//		    		Result r = it2.next();
//		    		if(index==1)
//					System.out.println("3:"+r.get("orderid"));
//					
//				}
//		    	
//			}
//		}
//		
//	}).start();
		
//new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				while (true) {
//					Iterator<Result> it2 = os.queryOrdersByBuyer(1470611363, 1484693606, "ap-ab95-3e7e0ed47717");
//			    	int index = 0;
//			    	while (it2.hasNext()) {
//			    		index++;
//						it2.next();
//						
//					}
//			    	System.out.println("2: "+index);
//				}
//				
//			}
//		}).start();
//		
//		while (wait) {
		Thread.sleep(10000);
//		}
    	
//	
    	
    	//		int index=0;
//		while (it2.hasNext()) {
//			index++;
//			it2.next();
//		}
//		System.out.println(index);
//		
//		it2 = os.queryOrdersBySaler("ay-a576-84cc0ef460d3", "gd-8870-0537e54f51ca",keyy );
//		 index=0;
//		while (it2.hasNext()) {
//			index++;
//			it2.next();
//		}
//		System.out.println(index);
		
	
		
		
		String buyerid = "tp-afba-4e1ef98c8590";
		long startTime = 0;
		long endTime = 2094693606;
		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		Iterator<Result> it = os
//				.queryOrdersByBuyer(startTime, endTime, buyerid);
//		while (it.hasNext()) {
//
////		System.out.println(it.next());
//	}
		System.exit(0);
//		int index=0;
//		while (it.hasNext()) {
//			index++;
//			System.out.println(it.next());
//		}
//		System.out.println(index);
			
		// 用例
		long orderid = 2982388;
		System.out.println("\n查询订单号为" + orderid + "的订单");
		System.out.println(os.queryOrder(orderid, null));

		System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
		System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

		System.out.println("\n查询订单号为" + orderid
				+ "的订单的contactphone, buyerid, foo, done, price字段");
		List<String> queryingKeys = new ArrayList<String>();
		queryingKeys.add("contactphone");
		queryingKeys.add("buyerid");
		queryingKeys.add("foo");
		queryingKeys.add("done");
		queryingKeys.add("price");
		Result result = os.queryOrder(orderid, queryingKeys);
		System.out.println(result);
		
//		long s1 = System.currentTimeMillis();
		System.out.println("\n查询订单号不存在的订单");
		result = os.queryOrder(1111, queryingKeys);
		if (result == null) {
			System.out.println(1111 + " order not exist");
		}
//		long e1 = System.currentTimeMillis();
//		System.out.println(s1 - e1);
		
//		long s2 = System.currentTimeMillis();
//		System.out.println("\n查询订单号不存在的订单");
//		result = os.queryOrderCBF(1111, queryingKeys);
//		if (result == null) {
//			System.out.println(1111 + " order not exist");
//		}
//		long e2 = System.currentTimeMillis();
//		System.out.println("" + (s2 - e2));


//		String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
//		long startTime = 1471025622;
//		long endTime = 1471219509;
//		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		Iterator<Result> it = os
//				.queryOrdersByBuyer(startTime, endTime, buyerid);
//		while (it.hasNext()) {
//			System.out.println(it.next());
//		}

		
//
//		goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
//		String attr = "app_order_33_0";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		System.out.println(os.sumOrdersByGood(goodid, attr));
//
//		attr = "done";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		KeyValue sum = os.sumOrdersByGood(goodid, attr);
//		if (sum == null) {
//			System.out.println("由于该字段是布尔类型，返回值是null");
//		}
//
//		attr = "foo";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		sum = os.sumOrdersByGood(goodid, attr);
//		if (sum == null) {
//			System.out.println("由于该字段不存在，返回值是null");
//		}
//	
	}

}
