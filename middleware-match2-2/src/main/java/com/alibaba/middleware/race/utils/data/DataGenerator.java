package com.alibaba.middleware.race.utils.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Random;


/**
 *@Description 模拟数据输出
 *@Author mindw
 *@Since 2016年7月11日
 *@Version 0.0.1
 */
public class DataGenerator {
	
	public static void main(String[] args) throws IOException {
//		// 买家数
//		int buyerNum = 20000000;
//		// 卖家数
//		int salerNum = 1000000;
//		// 每个卖家多少件商品
//		int goodNumPerSaler = 10;
//		// 商品数
//		int goodNum = salerNum * goodNumPerSaler;
//		// 每个买家买多少件商品
//		int orderPerBuyer = 30;
		
		// 买家数
		int buyerNum = 3000000;
		// 卖家数
		int salerNum = 4000;
		// 每个卖家多少件商品
		int goodNumPerSaler = 100;
		// 商品数
		int goodNum = salerNum * goodNumPerSaler;
		// 每个买家买多少件商品
		int orderPerBuyer = 5;
		
		// 写入买家数据：取消下面两行的注释
//		new BuyerGenerator().produce(buyerNum,"E:/data/buyer_records/");
//		System.out.println("BuyerGenerator finished! 记录数："+buyerNum);
		
		// 测试写入是否成功
		//Collection<String> files = Arrays.asList(new String[]{"./data/buyer_records/buyer_records.txt"});
		//handle(files);
		
		// 写入商品数据：取消下面两行的注释
//		new GoodGenerator().produce(salerNum,goodNumPerSaler,"E:/data/good_records/");
//		System.out.println("GoodGenerator finished! 记录数："+goodNum);
		
		// 测试写入是否成功
		//Collection<String> files = Arrays.asList(new String[]{"./data/good_records/0.txt"});
		//handle(files);

		// 写入订单数据：取消下面两行的注释（耗时较久）,如果只是测试可以缩小传入参数
		new OrderGenerator().produce(buyerNum, goodNum, orderPerBuyer, "E:/data/order_records/");
		System.out.println("OrderGenerator finished! 记录数："+(buyerNum * orderPerBuyer));
		
		// 测试写入是否成功
		//Collection<String> files = Arrays.asList(new String[]{"./data/order_records/0.txt"});
		//handle(files);
	}
	
	
	/**
	 * 每个文件最多的记录条数: 100万条
	 */
	private static final int BUYER_NUM_PER_FILE = 1000000;
	
	/**
	 * 买家数据生成器
	 */
	static class BuyerGenerator {
		
		// >>> 一些字段
		// buyerid contactphone
		// recieveaddress 
		// buyername
		// app_buyer_41_0 44150.4
		// app_buyer_76_0 18061.1
		// app_buyer_112_0 42885.9
		
		/**
		 * 买家最多的个数：1亿
		 */
		private static final int MAX_GENERATE_BUYER_NUM = 100000000; 
		
		/**
		 * id前缀：偶数 ap_ ，奇数tb_
		 */
		private static String[] buyerid_prefixes = {"ap_","tb_"}; 
		
		/**
		 * @param generateBuyerNum 要产生的买家数，最大不超过  {@link #MAX_GENERATE_BUYER_NUM}
		 * @param outFileFolder 输出文件目录 
		 */
		public void produce(int generateBuyerNum,String outFileFolder){
			
			// 输出数目
			generateBuyerNum = Math.min(generateBuyerNum, MAX_GENERATE_BUYER_NUM);
			
			Random random = new Random(0);
			
			PrintWriter writer = null;
			
			for(int id=0;id<generateBuyerNum;++id){
				
				if(id%BUYER_NUM_PER_FILE==0){ // 达到每个文件的输出个数
					
					if(writer!=null)
						writer.close();
					int index = id / BUYER_NUM_PER_FILE;
					// 输出文件名
					String outFilePath = outFileFolder+index+".txt";
					writer = IOUtil.getPrintWriter(outFilePath);
				}
				
				StringBuffer buffer = new StringBuffer();
				
				buffer.append("buyerid:"+getBuyerId(id)+"\t");
				
				String contactphone = String.format("137%08d", id);
				buffer.append("contactphone:"+contactphone+"\t");
				
				String recieveaddress = String.format("孝陵卫%08d号",id);
				buffer.append("recieveaddress:"+recieveaddress+"\t");
				
				String buyername = String.format("天字号%08d",id);
				buffer.append("buyername:"+buyername+"\t");
						
				String price = String.format("%.1f",random.nextDouble() *100000.0);
				
				// 实例中的一些字段
				if(id%4==0){
					buffer.append("app_buyer_41_0:"+price+"\n");
				}else if(id%4==1){
					buffer.append("app_buyer_76_0:"+price+"\n");
				}else if(id%4==2){
					buffer.append("app_buyer_112_0:"+price+"\n");
				}else{
					buffer.append("\n");
				}
					
				writer.write(buffer.toString());
				//System.out.print(buffer.toString());
			}
			
			if(writer!=null)
				writer.close();
		}

		public static String getBuyerId(int buyerid) {
			return String.format("%s%036d", buyerid_prefixes[buyerid%2],buyerid);
		}
	
	}
	
	/**
	 * 货物数据生成器
	 */
	static class GoodGenerator {
		
		// >>> 一些字段
//		goodid aliyun_436e22cf-3cb7-438a-a741-73d0100c987d
//		salerid tb_43bd05cd-9995-45be-962d-0cf701d107ec
//		good_name 盐酸盐传誉
//		price 33.73
//		offprice 9.06
//		goodname 加腾安安定定诽谤性新罗区有赖李传卿卫道维多利诺北京西单文化广场懂高中生
//		app_good_3334_1 90
//		app_good_3334_2 64109.3
//		app_good_3334_3 64941.8
//		app_good_3334_5 62551.2
		
		/**
		 * 卖家id前缀
		 */
		private String[] salerid_prefixes = {
				"almm_",
				"tm_",
				"tb_",
				"wx_"};  
		
		/**
		 * 货物id前缀
		 */
		private static String[] goodid_prefixes = {
				"good_",
				"goodal_",
				"goodxn_",
				"goodtb_",
				"aliyun_"};
		
		/**
		 * 卖家最多的个数：1千万
		 */
		private static final int MAX_GENERATE_SALER_NUM = 10000000; 
		
		/**
		 * 每个卖家最多多少个货物： 20个
		 */
		private static final int MAX_GOOD_NUM_PER_SALER = 20; 
		
		/**
		 * @param generateSalerNum 要产生的卖家数 ,最大不超过 {@link #MAX_GENERATE_SALER_NUM}
		 * @param goodNumPerSaler 每个卖家有多少货物,最大不超过 {@link #MAX_GOOD_NUM_PER_SALER}
		 * @param outFileFolder 输出根目录
		 */
		public void produce(int generateSalerNum,int goodNumPerSaler, String outFileFolder) {
			
			generateSalerNum = Math.min(generateSalerNum, MAX_GENERATE_SALER_NUM);
			goodNumPerSaler = Math.min(goodNumPerSaler, MAX_GOOD_NUM_PER_SALER);
			int generateGoodNum = generateSalerNum * goodNumPerSaler;
			
			Random random = new Random(0);
			
			PrintWriter writer = null;
			
			for(int goodid=0;goodid<generateGoodNum;++goodid){
				
				if(goodid%BUYER_NUM_PER_FILE==0){ // 达到每个文件的输出个数
					
					if(writer!=null)
						writer.close();
					int index = goodid / BUYER_NUM_PER_FILE;
					// 输出文件名
					String outFilePath = outFileFolder+index+".txt";
					writer = IOUtil.getPrintWriter(outFilePath);
				}
				
				int salerid = goodid % generateSalerNum; // 为了把记录分散开
				
				StringBuffer buffer = new StringBuffer();
				
				buffer.append("goodid:"+getGoodId(goodid)+"\t");
				
				buffer.append("salerid:"+getSalerId(salerid)+"\t");
				
				buffer.append(String.format("goodname:货%011d\t",goodid));
				
				String price = String.format("%.2f",random.nextDouble() *10000.0);
				buffer.append("price:"+price+"\t");
				
				String offprice = String.format("%.2f",random.nextDouble() *100.0);
				buffer.append("offprice:"+offprice+"\t");
				
				if(goodid%4==0)
					buffer.append("goodname:填充样例goodname\t");
				
				buffer.append(String.format(
						"app_good_3334_%d:%.2f\n",
						goodid % 5,
						random.nextDouble() * 10000.0)
						);
				
				writer.write(buffer.toString());
				
			}
			
			if(writer!=null)
				writer.close();
			
		}
		
		public static String getGoodId(int id){
			return String.format("%s%036d", goodid_prefixes[id%5],id);
		}
		
		private String getSalerId(int id){
			return String.format("%s%036d", salerid_prefixes[id%4],id);
		}
		
	}

	/**
	 * 订单数据生成器
	 */
	static class OrderGenerator {
		
		// >>> 一些字段
//		orderid 3009309
//		goodid good_b74b0669-f947-4e7c-aa71-2f0bcd008602
//		buyerid tb_bfbacb95-5e0c-4b4e-bfe8-f38a54019eae
//		createtime 1471274541
//		done true
//		amount 211
//		app_order_76_0 12675.62
//		app_order_76_1 5942.3
//		app_order_33_2 8237.38
//		app_order_3334_1 102
//		app_order_3334_2 47905.3
		
		// 1467302400 2016.7.1
		// 10ms一次
		
		private long createtime = 1467302400000L; // 1467302400 2016.7.1
		
		
		/**
		 * @param buyerNum 买家数目
		 * @param goodNum 货物数目
		 * @param orderPerBuyer 每个买家买多少货物
		 * @param outFileFolder 输出目录
		 */
		public void produce(int buyerNum,int goodNum,int orderPerBuyer, String outFileFolder) {
			long orderNum = buyerNum * orderPerBuyer;
			
			Random random = new Random(0);
			
			PrintWriter writer = null;
			
			for(long orderid = 0;orderid<orderNum;++orderid){
				
				if(orderid%BUYER_NUM_PER_FILE==0){ // 达到每个文件的输出个数
					
					if(writer!=null)
						writer.close();
					long index = orderid / BUYER_NUM_PER_FILE;
					// 输出文件名
					String outFilePath = outFileFolder+index+".txt";
					writer = IOUtil.getPrintWriter(outFilePath);
				}
			
				StringBuffer buffer = new StringBuffer();
				
				buffer.append("orderid:"+orderid+"\t");
				
				int goodid = (int)(orderid % goodNum);
				buffer.append("goodid:"+GoodGenerator.getGoodId(goodid)+"\t");
				
				int buyerid = (int)(orderid % buyerNum);
				buffer.append("buyerid:"+BuyerGenerator.getBuyerId(buyerid)+"\t");
				
				createtime += 10; // 10ms递增
				buffer.append(String.format("createtime:%d\t",(createtime / 1000)));
				
				if(orderid%4==0)
					buffer.append("done:false\t");
				else
					buffer.append("done:true\t");
				
				buffer.append("amount:"+(random.nextInt(1000)+1)+"\t");
				
				buffer.append(String.format(
						"app_order_76_%d:%.2f\n",
						orderid % 5,
						random.nextDouble() * 10000.0)
						);
				
				
				writer.write(buffer.toString());
			}
			
			
			if(writer!=null)
				writer.close();
		}
		
	
	}
	

	/// >>> 以下是读入，与输出无关，为了测试输出是否可读
	static void handle(Collection<String> files) throws IOException {
		for (String file : files) {
			BufferedReader bfr = createReader(file);
			try {
				String line = bfr.readLine();
				while (line != null) {	
					System.out.println("---------");
					
					String[] kvs = line.split("\t");
					for (String rawkv : kvs) {
						int p = rawkv.indexOf(':');
						String key = rawkv.substring(0, p);
						String value = rawkv.substring(p + 1);
						
						if (key.length() == 0 || value.length() == 0) {
							throw new RuntimeException("Bad data:" + line);
						}
						
//						if(key.equals("buyerid"))
//							System.out.println(key+" "+value.substring(0,3));
					
						System.out.println(key+" "+value);
						
					}
					
					line = bfr.readLine();
				}
			} finally {
				bfr.close();
			}
		}
	}
	
	private static BufferedReader createReader(String file)
			throws FileNotFoundException {
		return new BufferedReader(new FileReader(file));
	}
	
}
