package com.alibaba.middleware.race.file;

/**
 * 原始数据
 */
public class Index {	

	/**
	 * 索引文件地址
	 */
	public String indexFileName;
	
	/**
	 * 索引内容： 文件名，pos,offset\n
	 */
	public String content;

	
	public Index(String indexFileName,String key,String dataFileName,long pos,int len) {
		super();
		this.indexFileName = indexFileName;
		this.content = String.format("%s,%s,%d,%d\n",key,dataFileName,pos,len);
	}
	
}
