package com.alibaba.middleware.race.file;

/**
 * 统计信息
 */
public class Info{
	
	/**
	 * 文件地址
	 */
	public String fileName;
	
	/**
	 * 要写的内容的key: "a_id1:b_id1" 
	 */
	public String key;

	/**
	 * 要写的内容的value: "a_id1:b_id1" 
	 */
	public String value;

	public Info(String fileName, String key, String value) {
		super();
		this.fileName = fileName;
		this.key = key;
		this.value = value;
	}
	
}
