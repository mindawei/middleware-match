/**
 * Copyright (c) 2015-2016, Silly Boy 胡建洪(1043244432@qq.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.middleware.race;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.file.FileMapper;

/**
 *
 *
 * @author JianhongHu
 * @version 1.0
 * @date 2016年7月9日
 */
public class TestMain {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		//String basePath = "prerun_data/";
		String basePath = "data/test/";
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
		storeFolders.add(basePathOut);

		OrderSystemImpl os = new OrderSystemImpl();
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

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


		String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
		long startTime = 1471025622;
		long endTime = 1471219509;
		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
		Iterator<Result> it = os
				.queryOrdersByBuyer(startTime, endTime, buyerid);
		while (it.hasNext()) {
			System.out.println(it.next());
		}

		String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
		String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
		System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
		it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
		while (it.hasNext()) {
			System.out.println(it.next());
		}

		goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
		String attr = "app_order_33_0";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		System.out.println(os.sumOrdersByGood(goodid, attr));

		attr = "done";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		KeyValue sum = os.sumOrdersByGood(goodid, attr);
		if (sum == null) {
			System.out.println("由于该字段是布尔类型，返回值是null");
		}

		attr = "foo";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		sum = os.sumOrdersByGood(goodid, attr);
		if (sum == null) {
			System.out.println("由于该字段不存在，返回值是null");
		}
	}

}
