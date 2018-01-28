//package com.alibaba.middleware.race.sync;
//
//import java.io.File;
//import java.io.RandomAccessFile;
//import java.util.concurrent.Semaphore;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///***
// * 测试读数据
// * 
// * @author mindw
// */
//public final class Reader {
//
//	private static final Logger logger = LoggerFactory.getLogger(Server.class);
//
//	public static void main(String[] args) {
//		Reader.readMessage();
//	}
//
//	// data 32 * 1024 * 1024 * 10 = 320M
//	// map1 10240000 * 8 = 80M
//	// map2 1024 * 12 * 1024 * 16 = 192M
//	// item 100 * 2048 * 17 = 3.4M
//	static final long[] readMessage() {
//		long t1 = System.currentTimeMillis();
//		new ReadThread().start();
//		new ParseThread().start();
//		long[] map1 = null;
//		try {
//			map1 = store();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		logger.info("Read time cost:" + (System.currentTimeMillis() - t1));
//		return map1;
//	}
//
//	/// >> 解析部分
//	private static final byte MAN_TAG = -25;
//	private static final int MAX_MSG_LEN = 200;
//
//	private static final int DATA_BUFF_SIZE = 16 * 1024 * 1024;
//	private static final int DATA_QUEUE_SIZE = 20;
//	private static final Semaphore DATA_EMPTY = new Semaphore(DATA_QUEUE_SIZE);
//	private static final Semaphore DATA_FULL = new Semaphore(0);
//
//	private static final byte[][] datas = new byte[DATA_QUEUE_SIZE][DATA_BUFF_SIZE + MAX_MSG_LEN]; // 给之前的预留
//	private static final int[] dataLens = new int[DATA_QUEUE_SIZE];
//	
//	private static final class ReadThread extends Thread {
//		@Override
//		public void run() {
//
//			RandomAccessFile file = null;
//			int queTail = 0, off,len,totalSize;
//			byte[] curData;
//			
//			final String basePath = Constants.DATA_HOME + File.separator;
//			try {
//				
//				for (int fileIndex = 1; fileIndex <= 10; ++fileIndex) {
//					
//					file = new RandomAccessFile(basePath + fileIndex + ".txt", "r");
//					totalSize = (int)file.length();
//					off = 0;
//				
//					for(;;) {
//						
//						curData = datas[queTail];
//						
//						DATA_EMPTY.acquire();
//						
//						len = file.read(curData, 0, DATA_BUFF_SIZE);
//						
//						// 确保有换行符
//						if(curData[len - 1] != '\n'){
//							while((curData[len++] = (byte)file.read())!='\n');
//						}
//						
//						// 设置真正长度
//						dataLens[queTail] = len;
//						
//						// 释放该区域
//						DATA_FULL.release();
//						
//						queTail++;
//						if (queTail == DATA_QUEUE_SIZE) {
//							queTail = 0;
//						}
//						
//						off += len;
//						if(off == totalSize){
//							break;
//						}
//
//					}
//					
////					file.close();
//				}
//
//				// 结束
//				DATA_EMPTY.acquire();
//				// 结束信号
//				dataLens[queTail] = 0;
//				DATA_FULL.release();
//			
//				logger.info("DATA_FULL:"+DATA_FULL.availablePermits());
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//
//	private static final class ParseThread extends Thread {
//		@Override
//		public void run() {
//			try {
//				deal();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
////	private static boolean isOnline = false;
//
//	/**
//	 * 解析文件
//	 * 
//	 * @throws InterruptedException
//	 */
//	private static final void deal() throws InterruptedException {
//
//		int limit = 0, bg, pk, new_pk, off = 19, pos = 0, v;
//		long item;
//		byte ch;
//		int name;
//
//		int insert_pk_off = 15;
//		int insert_first_name_off = insert_pk_off + 21;
//		
//		final int twoBase = 11 * '0';
//
//		ITEM_EMPTY.acquire();
//		byte[] ops = ITEM_OPS[0];
//		int[] update_datas = ITEM_UPDATE_DATAS[0];
//		int[] pks = ITEM_PKS[0];
//		long[] items = ITEM_ITEMS[0];
//		int len = 0;
//		int itemHead = 1;
//		int nextPos;
//		
//		DATA_FULL.acquire();		
//		byte[] line = datas[0];
//		pos = 0;
//		limit = dataLens[0];
//		int queHead = 1;
//
//		for (;;) {
//
//			// 忽略 |mysql-bin.000018604199565|
//			pos += off;
//			ch = line[pos];
//			if (ch != 'U' && ch != 'D' && ch != 'I') {
//
//				pos -= off;
//				bg = pos;
//
//				pos += 19;
//				while (line[pos++] != '|');
//
//				// timestamp|scheme|table|
//				pos += 34;
//				off = pos - bg;
//				ch = line[pos];
//			}
//
//			if (ch == 'U') {
//
//				// U|id:1:1|
//				bg = (pos = pos + 9);
//
//				pk = line[pos++] - '0';
//				while ((ch = line[pos++]) != '|') {
//					pk = (pk << 3) + (pk << 1) + (ch - '0');
//				}
//				pks[len] = pk;
//
//				// 如果不更新主键，那么下一个主键长度可以预测 ,再移动4位, 防止数组越界的误判
//				// 防止与|mysql-bin.误判 : score -> e first_name -> t last_name -> _
//				ch = (nextPos = pos - bg + pos + 4 ) < limit ? line[nextPos] : (byte)'N';
//				
//				// mysql.bin
//				// 取出 key
//				if (ch == 'e') { // scor e
//
//					ops[len] = op_update_score;
//
//					// 跳过主键计算 | score:1:0| 至少2位
//					pos = nextPos + 8;
//
//					// 旧值 |
//					while (line[pos++] != '|');
//
//					// 取出新的值,2-4位
//					v = ((v = line[pos++]) << 3) + (v << 1) + line[pos++] - twoBase;
//					if(line[pos]=='|'){ // 2位
//						// ignore
//					}else if(line[pos+1]=='|'){ // 3位
//						v = (v << 3) + (v << 1) + (line[pos++] - '0'); 
//					}else{ // 4位
//						v = (v<<6)+(v<<5)+(v<<2) + ((v = line[pos++]) << 3) + (v << 1) + line[pos++] - twoBase;
//					}
//					
//					update_datas[len++] = v;
//					
//					pos+=2; // 跳过 | \n
//
//				} else if (ch == 't') { // firs t_name
//
//					ops[len] = op_update_firsrt_name;
//
//					// 跳过主键计算 | first_name:2:0| 3位 | 
//					items[len++] = ( (long) (((line[nextPos + 16] << 5) ^ line[nextPos + 17]) & CHINESE_SIZE) ) << FIRST_NAME_OFF;
//					pos = nextPos + 20; // 跳过\n
//
//				} else if (ch == '_') { // last _name
//
//					ops[len] = op_update_last_name;
//
//					// 跳过主键计算 | last_name:2:0| 至少3位 旧值 |
//					if (line[nextPos + 13] == '|'){
//						pos = nextPos + 14;
//					}else{
//						pos = nextPos + 17;
//					}
//					
//					name = ((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE;
//					
//					if (line[pos + 3] == '|') {
//						pos += 5; // 跳过\n
//					} else {
//						name |= ((((line[pos + 4] << 5) ^ line[pos + 5]) & CHINESE_SIZE) << 10);
//						pos += 8; // 跳过\n
//					}
//					
//					items[len++] = (((long)name) << LAST_NAME_1_OFF);
//
//				} else {
//
//					ops[len] = op_update_pk;
//
//					// 取出新整数部分
//					new_pk = line[pos++] - '0';
//					while ((ch = line[pos++]) != '|') {
//						new_pk = (new_pk << 3) + (new_pk << 1) + (ch - '0');
//					}
//					update_datas[len++] = new_pk;
//					
//					++pos; // 跳过\n
//				}
//				
//			} else if (ch == 'D') {
//				// D|id:1:1|
//				pos += 9;
//
//				// 删除
//				pk = line[pos++] - '0';
//				while ((ch = line[pos++]) != '|') {
//					pk = (pk << 3) + (pk << 1) + (ch - '0');
//				}
//
//				// 删除范围内的
//				if (pk < 8000000 && pk > 1000000) {
//					ops[len] = op_delete;
//					pks[len++] = pk;
//				}
//
//				pos += 106; 
//				while (line[pos++] != '\n');
//
//			} else { // Insert
//
//				// I|id:1:1|NULL| 1-7 bytes |
//				// 主键递增
//				if (line[pos + insert_pk_off] != '|') {
//					++insert_pk_off;
//					++insert_first_name_off;
//				}
//				pos += insert_first_name_off; // insert_pk_off + 21;
//
//				// first_name:2:0|NULL| 3 bytes |
//				item = (((long)(((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE)) << FIRST_NAME_OFF);
//				
//				// last_name:2:0|NULL| 3 or 6 bytes |
//				item |= (((long)(((line[pos + 24] << 5) ^ line[pos + 25]) & CHINESE_SIZE)) << LAST_NAME_1_OFF);
//
//				if (line[pos + 26] == '|') {
//					pos += 40;
//				} else {
//					item |= (((long)(((line[pos + 27] << 5) ^ line[pos + 28]) & CHINESE_SIZE)) << LAST_NAME_2_OFF);
//					pos += 43;
//				}
//
//				// sex:2:0|NULL|
//				if (line[pos] == MAN_TAG) {
//					item |= SEX_TAG;
//				}
//
//				// 3 bytes | score:1:0|NULL| 2-4 bytes |
//				pos += 19;
//				v = ((v = line[pos++]) << 3) + (v << 1) + line[pos++] - twoBase;
//				while ((ch = line[pos++]) != '|') {
//					v = (v << 3) + (v << 1) + (ch - '0');
//				}
//				item |= v;
//
//				// score2:1:0|NULL| 2-6 bytes |
//				pos += 16;
//				v = ((v = line[pos++]) << 3) + (v << 1) + line[pos++] - twoBase;
//				while ((ch = line[pos++]) != '|') {
//					v = (v << 3) + (v << 1) + (ch - '0');
//				}
//				// 不强制转不能完成正常的移位
//				item |= (((long)v) << SCORE2_OFF);
//
//				++pos; // 跳过 \n
//
//				ops[len] = op_insert;
//				items[len++] = item;
//			}
//
//			if (len == BATCH_SIZE) {
//				
//				ITEM_FULL.release();
//				
//				len = 0;
//				ops = ITEM_OPS[itemHead];
//				update_datas = ITEM_UPDATE_DATAS[itemHead];
//				pks = ITEM_PKS[itemHead];
//				items = ITEM_ITEMS[itemHead++];
//
//				if (itemHead == ITEM_QUEUE_SIZE) {
//					itemHead = 0;
//				}
//				
//				ITEM_EMPTY.acquire();
//			}
//			
//			
//			// load	
//			if (pos == limit) {
//				
//				DATA_EMPTY.release();
//
//				pos = 0;
//				line = datas[queHead];
//				
//				DATA_FULL.acquire();
//	
//				if ((limit = dataLens[queHead++]) == 0) {
//					ops[len] = op_end;
//					ITEM_FULL.release();
//					logger.info("ITEM_FULL:"+ITEM_FULL.availablePermits());
//					return;
//				}
//			
//				if (queHead == DATA_QUEUE_SIZE) {
//					queHead = 0;
//				}
//			}
//			
//		}
//
//	}
//
//	/// >> map 处理
//	private static final int ITEM_QUEUE_SIZE = 400;
//	private static final int BATCH_SIZE = 2048;
//
//	private static final Semaphore ITEM_EMPTY = new Semaphore(ITEM_QUEUE_SIZE);
//	private static final Semaphore ITEM_FULL = new Semaphore(0);
//
//	private static final byte op_update_pk = 0;
//	private static final byte op_update_score = 1;
//	private static final byte op_update_firsrt_name = 2;
//	private static final byte op_update_last_name = 3;
//	
//	private static final byte op_delete = 4;
//	private static final byte op_insert = 5;
//	private static final byte op_end = 6;
//
//	private static final byte[][] ITEM_OPS = new byte[ITEM_QUEUE_SIZE][BATCH_SIZE];
//	private static final int[][] ITEM_PKS = new int[ITEM_QUEUE_SIZE][BATCH_SIZE];
//	private static final long[][] ITEM_ITEMS = new long[ITEM_QUEUE_SIZE][BATCH_SIZE];
//	private static final int[][] ITEM_UPDATE_DATAS = new int[ITEM_QUEUE_SIZE][BATCH_SIZE];
//	
//
//	private static long[] store() throws InterruptedException {
//
//		final int HIGH_BIT = 10;
//		final int CAP = 10240000;// 8000000; 扩大一倍稍微慢了一点
//		int lastInsertPk = 0;
//
//		final long[] map1 = new long[CAP];
//		final int SIZE = (1 << 14) - 1; // 2 << 12-1
//		final IntLongHashMap[] map2 = new IntLongHashMap[SIZE + 1];
//		for (int i = 0; i < map2.length; ++i) {
//			map2[i] = new IntLongHashMap(1024);
//		}
//		final long NULL = 0;
//
//		byte[] ops;
//		int[] update_datas;
//		int[] pks;
//		long[] items;
//
//		byte op;
//		int new_pk, pk, i;
//		long item;
//		int itemTail = 0;
//
//		for (;;) {
//
//			ops = ITEM_OPS[itemTail];
//			update_datas = ITEM_UPDATE_DATAS[itemTail];
//			pks = ITEM_PKS[itemTail];
//			items = ITEM_ITEMS[itemTail];
//			
//			ITEM_FULL.acquire();
//		
//
//			for (i = 0; i < BATCH_SIZE; ++i) {
//				op = ops[i];
//
//				if (op < op_delete) {
//
//					pk = pks[i];
//					
//					if (op == op_update_pk) { // 更新主键
//
//						new_pk = update_datas[i];
//
//						if (pk < CAP) {
//							item = map1[pk];
//							map1[pk] = NULL;
//						} else {
//							item = map2[(pk >>> HIGH_BIT) & SIZE].get(pk);
//						}
//
//						if (new_pk < CAP) {
//							map1[new_pk] = item;
//						} else {
//							map2[(new_pk >>> HIGH_BIT) & SIZE].put(new_pk, item);
//						}
//
//					} else { // 更新其他键
//
//						if (pk < CAP) {
//							item = map1[pk];
//						} else {
//							item = map2[(pk >>> HIGH_BIT) & SIZE].get(pk);
//						}
//						
//						if (op == op_update_score) {
//			
//							item &= CLEAR_SCORE_TAG;
//							item |= ((long)update_datas[i]);
//
//						} else if (op == op_update_firsrt_name) {
//
//							item &= CLEAR_FIRST_NAME_TAG;
//							item |= items[i];
//						
//						} else {
//							
//							item &= CLEAR_LAST_NAME_TAG;
//							item |= items[i];
//						}
//						
//						if (pk < CAP) {
//							map1[pk] = item;
//						} else {
//							map2[(pk >>> HIGH_BIT) & SIZE].put(pk, item);
//						}
//
//					}
//					
//				} else if (op == op_delete) {
//
//					map1[pks[i]] = NULL;
//
//				} else if (op == op_insert) {
//
//					// pk = pks[i];
//					pk = ++lastInsertPk;
//					if (pk < CAP) {
//						map1[pk] = items[i];
//					} else {
//						map2[(pk >>> HIGH_BIT) & SIZE].put(pk, items[i]);
//					}
//
//				} else {
//
//					return map1;
//				}
//			}
//
//			ITEM_EMPTY.release();
//			
//			if ((itemTail++) == ITEM_QUEUE_SIZE) {
//				itemTail = 0;
//			}
//			
//		}
//	}
//	
//	/// >> bit
//	// 64位
//	// | score2 | sex |lastName_2| lastName_1 | firstName | score
//	// 19 1 10 10 10 14
//
//	private static final int SCORE2_OFF = 1 + 10 + 10 + 10 + 14;
//	private static final int SEX_OFF = 10 + 10 + 10 + 14;
//	private static final int LAST_NAME_2_OFF = 10 + 10 + 14;
//	private static final int LAST_NAME_1_OFF = 10 + 14;
//	private static final int FIRST_NAME_OFF = 14;
//	//private static final int SCORE_OFF = 0;
//
//	private static final long SCORE_TAG = bits(0, FIRST_NAME_OFF);
//	private static final long FIRST_NAME_TAG = bits(FIRST_NAME_OFF, LAST_NAME_1_OFF);
//	private static final long LAST_NAME_1_TAG = bits(LAST_NAME_1_OFF, LAST_NAME_2_OFF);
//	private static final long LAST_NAME_2_TAG = bits(LAST_NAME_2_OFF, SEX_OFF);
//	private static final long SEX_TAG = bits(SEX_OFF, SCORE2_OFF);
//	//private static final long SCORE2_TAG = 0xFFFFE00000000000L;
//
//	private static final long CLEAR_SCORE_TAG = ~SCORE_TAG;
//	private static final long CLEAR_LAST_NAME_TAG = ~(LAST_NAME_2_TAG | LAST_NAME_1_TAG);
//	private static final long CLEAR_FIRST_NAME_TAG = ~FIRST_NAME_TAG;
//
//	private static final int CHINESE_SIZE = 1023;
//
//	// [ , )
//	private static final long bits(int from, int to) {
//		return ((1L << to) - 1) ^ ((1L << from) - 1);
//	}
//
//}