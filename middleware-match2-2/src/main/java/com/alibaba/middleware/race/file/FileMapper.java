package com.alibaba.middleware.race.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.file.Constructor.FileInfo;

/**
 * 文件映射类
 */
public class FileMapper {
	
	private static FileMapper instance = new FileMapper();
	
	private FileMapper(){}
	
	public static FileMapper getInstance(){
		return instance;
	}
	
//	order num:400000000
//	buyer num:8000000
//	good num: 4000000
	
//	private final int DATA_FILE_SIZE = 80;  //10 1024 9 	
//	
//	private final int INFO_FILE_SIZE = 800;  //11 2048 10 1024 512
//	
//	private final int INDEX_FILE_SIZE = 800; //11 2048 10 1024 512
//	
//	private int indexMod(String id){
//		int val = id.hashCode();
//		if(val<0)
//			val = -val;
//		return val % INDEX_FILE_SIZE;
//	}
//	
//	private long indexMod(long id){
//		return id % INDEX_FILE_SIZE;
//	}
//	
//	private int infoMod(String id){
//		int val = id.hashCode();
//		if(val<0)
//			val = -val;
//		return val % INFO_FILE_SIZE;
//	}
//	
//	
//	private int dataMod(String id){
//		int val = id.hashCode();
//		if(val<0)
//			val = -val;
//		return val % DATA_FILE_SIZE;
//	}
//	
//	private long dataMod(long id){
//		return id % DATA_FILE_SIZE;
//	}
	
	
	
	/// >>> 买家相关
	
	//	buyer num: 8000 000
	
	public String getBuyerDataIndexPath(String buyerid) {
		int hashCode = buyerid.hashCode();
		if(hashCode<0)
			hashCode = -hashCode;
		String storeFolder = storeFolders.get(hashCode % storeFoldersNum);
		int fileIndex = hashCode % 1024;
		return storeFolder+buyerDataIndexPath+fileIndex;
	}
	
	
	/**
	 * 根据买家id获得存放统计信息（买家有哪些订单）的文件地址
	 */
	public String getBuyerInfoPath(String buyerid) {		
		int hashCode = buyerid.hashCode();
		if(hashCode<0)
			hashCode = -hashCode;
		String storeFolder = storeFolders.get(hashCode % storeFoldersNum);
		int fileIndex = hashCode % 1024;
		return storeFolder+buyerInfoPath+fileIndex;
	}
	
	
	/// >>> 商品相关
	
	//	good num: 4000 000
	
	/**
	 * 根据商品id获得存商品数据的索引的文件地址
	 */
	public String getGoodDataIndexPath(String goodid) {	
		int hashCode = goodid.hashCode();
		if(hashCode<0)
			hashCode = -hashCode;
		String storeFolder = storeFolders.get(hashCode % storeFoldersNum);
		int fileIndex = hashCode % 512;
		return storeFolder+goodDataIndexPath+fileIndex;
	}
	

	/**
	 * 根据商品id获得存放统计信息（商品有哪些订单）的文件地址
	 */
	public String getGoodInfoPath(String goodid) {
		int hashCode = goodid.hashCode();
		if(hashCode<0)
			hashCode = -hashCode;
		String storeFolder = storeFolders.get(hashCode % storeFoldersNum);
		int fileIndex = hashCode % 512;
		return storeFolder+goodInfoPath+fileIndex;
	}
	

	/// >>> 订单相关
	
    //	order num:400 000 000  
	public String getOrderDataIndexPath(long orderId) {
		String storeFolder = storeFolders.get((int)(orderId % storeFoldersNum));
		int fileIndex = (int)(orderId % 1024);
		return storeFolder+orderDataIndexPath+fileIndex;
	}
	
	public String getOrerInfoPath(long orderId) {
		String storeFolder = storeFolders.get((int)(orderId % storeFoldersNum));
		int fileIndex = (int)(orderId % 1024);
		return storeFolder+orderInfoPath+fileIndex;
	}
	
	
	/** 存储文件 */
	private int storeFoldersNum;
	private List<String> storeFolders;

	private String buyerDataIndexPath = "b/";
	private String goodDataIndexPath ="g/";
	private String orderDataIndexPath = "o/";
	private String buyerInfoPath = "ib/";	
	private String goodInfoPath = "ig/";
	private String orderInfoPath = "io/";

	
	private Map<String,String> fileNameMap = new HashMap<String,String>();
	
	public String getFileNameByIndex(String index){
		return fileNameMap.get(index);
	}
	
	
	/**
	 * 创建所有的文件目录
	 */
	public void createDirs(List<FileInfo> fileInfos,Collection<String> storeFolders){
		
		for(int i=0;i<fileInfos.size();++i){
			fileNameMap.put(Integer.toString(i), fileInfos.get(i).fileName);
		}
		
		this.storeFolders = new ArrayList<>(storeFolders);
		storeFoldersNum = storeFolders.size();
		
		for (String storeFolder : storeFolders) {

			makeDirs(storeFolder + buyerDataIndexPath);

			makeDirs(storeFolder + goodDataIndexPath);

			makeDirs(storeFolder + orderDataIndexPath);

			makeDirs(storeFolder + buyerInfoPath);

			makeDirs(storeFolder + goodInfoPath);
			
			makeDirs(storeFolder + orderInfoPath);
			
		}
		
	}
	
	/** 
	 * 创建指定文件夹
	 */
	public void makeDirs(String folderName) {
		new File(folderName).mkdirs();
	}

	/**
	 * /disk1/orders/order.0.3  缩短成   1o03
	 * /disk2/buyers/buyer.0.0      1b00
	 * /disk3/goods/good.3.15     2b315
	 */
//	public String shortFileName(String fileName){
//		String[] segs = fileName.split("/");
//	
//		String diskName = segs[1];
//		char diskTag = diskName.charAt(diskName.length()-1);
//		
//		String typeName = segs[2];
//		char typeTag = typeName.charAt(0);
//		
//		String[] fileNames = segs[3].split("\\.");
//		
//		return String.format("%c%c%s%s", diskTag,typeTag,fileNames[1],fileNames[2]);
//	}

		
	
	
}
