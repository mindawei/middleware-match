package io.openmessaging.demo;

public class Global {
	
	/** 一个消息最大多少字节: 大部分消息小于100字节，但会偶尔插入几个大消息(小于100K) */
	static final int MAX_MESSAGE_SIZE = 2129 + 12; //2048 + 128; // 2129 + 12; // 12
				
	
	/** 多大 压缩 */
	static final int CMPRESS_SIZE = 30 * 1024 - 129; // 28 32 256k 48k
	
	/** 缓存大小  */
	static final int CACHE_SIZE = CMPRESS_SIZE + MAX_MESSAGE_SIZE; // 28 32 256k 48k
}
