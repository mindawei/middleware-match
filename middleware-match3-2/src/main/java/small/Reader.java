package small;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.IntLongHashMap;
import com.alibaba.middleware.race.sync.Server;

// Read time cost:39568
// Read time cost:32918
// Read time cost:33967

/***
 * 测试读数据
 * 
 * @author mindw
 */
public final class Reader {

	private static final Logger logger = LoggerFactory.getLogger(Server.class);

	public static void main(String[] args) {
		Reader.readMessage();
	}

	// data 32 * 1024 * 1024 * 10 = 320M
	// map1 10240000 * 8 = 80M
	// map2
	// item 100 * 2048 * 17 = 3.4M
	static final long[] readMessage() {
		long t1 = System.currentTimeMillis();
		new PreReadThread().start();
		new ReadThread().start();
		new ParseThread().start();
		long[] map1 = null;
		try {
			map1 = store();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("Read time cost:" + (System.currentTimeMillis() - t1));
		return map1;
	}

	/// >> 解析部分
	private static final byte MAN_TAG = -25;
	private static final int MAX_MSG_LEN = 200;

	private static final class PreReadThread extends Thread {
		@Override
		public void run() {

			ByteBuffer byteBuffer = null;
			RandomAccessFile mFile = null;
			FileChannel channel = null;

			int queTail = 0;

			final String baseFile = Constants.DATA_HOME + File.separator;

			try {
				for (int fileIndex = 1; fileIndex <= 10; ++fileIndex) {

					mFile = new RandomAccessFile(baseFile + fileIndex + ".txt", "r");
					channel = mFile.getChannel();

					System.out.println("begin " + fileIndex + ".txt");

					for (;;) {

						byteBuffer = buffers[queTail];
						DIRECT_EMPTY.acquire();

						byteBuffer.clear();
						if (channel.read(byteBuffer) <= 0) {
							// 并没有使用
							DIRECT_EMPTY.release();
							break;
						}
						byteBuffer.flip();

						DIRECT_FULL.release();
						if ((++queTail) == QUEUE_SIZE) {
							queTail = 0;
						}
					}

					mFile.close();
				}

				// 结束
				DIRECT_EMPTY.acquire();
				byteBuffer = buffers[queTail];
				// 结束信号
				byteBuffer.clear();
				byteBuffer.flip();
				DIRECT_FULL.release();
				
				logger.info("DIRECT_FULL:" + DIRECT_FULL.availablePermits());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static final int BUFF_SIZE = 8 * 1024 * 1024;
	private static final int QUEUE_SIZE = 20;

	private static final Semaphore DIRECT_EMPTY = new Semaphore(QUEUE_SIZE);
	private static final Semaphore DIRECT_FULL = new Semaphore(0);

	private static final ByteBuffer[] buffers = new ByteBuffer[BUFF_SIZE];
	static {
		for (int i = 0; i < QUEUE_SIZE; ++i) {
			buffers[i] = ByteBuffer.allocateDirect(BUFF_SIZE);
		}
	}

	private static final class ReadThread extends Thread {
		@Override
		public void run() {

			ByteBuffer byteBuffer;

			int bufPos = 0;
			int dataPos = 0;


			byte[] curData;
			int curLen;

			byte[] preData = null;
			int preLen = 0;
			int preOff = 0;

			try {
				for (;;) {

					byteBuffer = buffers[bufPos];
					curData = datas[dataPos];

					DIRECT_FULL.acquire();
					DATA_EMPTY.acquire();

					// 把之前的考到这里来
					if (preLen > 0) {
						System.arraycopy(preData, preOff, curData, 0, preLen);
					}

					// 读取现在的
					if ((curLen = byteBuffer.limit()) == 0) {
						break;
					}

					byteBuffer.get(curData, preLen, curLen);
					DIRECT_EMPTY.release();
					bufPos++;
					if (bufPos == QUEUE_SIZE) {
						bufPos = 0;
					}

					// 更新长度包括之前的部分
					curLen += preLen;

					// 判断现在部分后面有没有不完整的部分
					preOff = curLen - 1;
					while (curData[preOff--] != '\n');
					preOff += 2;

					// 如果有不完整的部分
					if ((preLen = curLen - preOff) > 0) {
						preData = curData;
						curLen -= preLen;
					}

					// 设置真正长度
					dataLens[dataPos] = curLen;

					// 释放该区域
					DATA_FULL.release();
					dataPos++;
					if (dataPos == QUEUE_SIZE) {
						dataPos = 0;
					}
				}
				
				
				if(preLen>0){
					// 设置真正长度
					dataLens[dataPos] = preLen;
					// 释放该区域
					DATA_FULL.release();
					
					if ((++dataPos) == QUEUE_SIZE) {
						dataPos = 0;
					}
				}
				
				
				// 结束
				DATA_EMPTY.acquire();
				// 结束信号
				dataLens[dataPos] = 0;
				DATA_FULL.release();

				logger.info("DATA_FULL:" + DATA_FULL.availablePermits());
				return;
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static final Semaphore DATA_EMPTY = new Semaphore(QUEUE_SIZE);
	private static final Semaphore DATA_FULL = new Semaphore(0);

	private static final byte[][] datas = new byte[QUEUE_SIZE][BUFF_SIZE + MAX_MSG_LEN]; // 给之前的预留
	private static final int[] dataLens = new int[QUEUE_SIZE];

	private static final class ParseThread extends Thread {
		@Override
		public void run() {
			try {
				deal();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// Read time cost:39649

	/**
	 * 解析文件
	 * 
	 * @throws InterruptedException
	 */
	private static final void deal() throws InterruptedException {

		// 2147483647
		// 2115058527
		// 9876543210

		final int[][] vals = new int[58][10];
		for (int i = '0'; i <= '9'; ++i) {
			int v = i - '0';
			for (int j = 0; j < 10; ++j) {
				if (j == 9 && i > '2') {
					break;
				}
				if (j == 0) {
					vals[i][j] = v;
				} else {
					vals[i][j] = vals[i][j - 1] * 10;
				}
			}
		}

		final int START = 100;
		final int END = 200;

		int pos, limit, bg, pk, old_pk, off = 19;
		long item, name, v;
		byte ch;

		DATA_FULL.acquire();
		byte[] line = datas[0];
		pos = 0;
		limit = dataLens[0];
		int queHead = 1;

		ITEM_EMPTY.acquire();
		byte[] ops = ITEM_OPS[0];
		int[] old_pks = ITEM_OLD_PKS[0];
		int[] pks = ITEM_PKS[0];
		long[] items = ITEM_ITEMS[0];
		int len = 0;
		int itemHead = 1;

		int insert_pk_off = 1;
		int i;

		for (;;) {

			// load
			if (pos == limit) {

				DATA_FULL.acquire();

				limit = dataLens[queHead];
				if (limit == 0) {
					ops[len] = op_end;
					ITEM_FULL.release();
					return;
				}

				pos = 0;
				line = datas[queHead];

				DATA_EMPTY.release();

				queHead++;
				if (queHead == QUEUE_SIZE) {
					queHead = 0;
				}
			}

			// 忽略 |mysql-bin.000018604199565|
			pos += off;
			ch = line[pos];
			if (ch != 'U' && ch != 'D' && ch != 'I') {

				pos -= off;
				bg = pos;

				pos += 19;
				while (line[pos++] != '|')
					;

				// timestamp|scheme|table|
				pos += 34;
				off = pos - bg;
				ch = line[pos];
			}

			if (ch == 'U') {

				// U|id:1:1|
				pos += 9;

				// 取出旧整数部分
				old_pk = line[pos++] - '0';
				while ((ch = line[pos++]) != '|') {
					old_pk = (old_pk << 3) + (old_pk << 1) + (ch - '0');
				}

				// 取出新整数部分
				pk = line[pos++] - '0';
				while ((ch = line[pos++]) != '|') {
					pk = (pk << 3) + (pk << 1) + (ch - '0');
				}

				// 根据旧的pk获得kv
				// item = (old_pk == pk) ? get(old_pk) : remove2(old_pk);
				item = 0;

				// 更新数据
				while ((ch = line[pos]) != '\n') {

					// 取出 key
					if (ch == 's') {

						// score:1:0| 至少2位
						pos += 12;

						// 旧值 |
						while (line[pos++] != '|')
							;

						// 取出新的值
						v = line[pos++] - '0';
						while ((ch = line[pos++]) != '|') {
							v = (v << 3) + (v << 1) + (ch - '0');
						}

						// item &= CLEAR_SCORE_TAG;
						item |= v;

					} else if (ch == 'f') {

						// first_name:2:0| 3位 |
						pos += 19;

						// item &= CLEAR_FIRST_NAME_TAG;
						name = ((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE;
						item |= (name << FIRST_NAME_OFF);
						pos += 4;

					} else {

						// last_name:2:0| 至少3位
						pos += 17;

						// 旧值 |
						pos += (line[pos] == '|') ? 1 : 4;

						// item &= CLEAR_LAST_NAME_TAG;
						name = ((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE;
						item |= (name << LAST_NAME_1_OFF);

						if (line[pos + 3] == '|') {
							pos += 4;
						} else {
							name = ((line[pos + 4] << 5) ^ line[pos + 5]) & CHINESE_SIZE;
							item |= (name << LAST_NAME_2_OFF);
							pos += 7;
						}
					}

				}

				ops[len] = op_update;
				old_pks[len] = old_pk;
				pks[len] = pk;
				items[len++] = item;

				if (len == BATCH_SIZE) {
					len = 0;

					ITEM_FULL.release();
					ITEM_EMPTY.acquire();

					ops = ITEM_OPS[itemHead];
					old_pks = ITEM_OLD_PKS[itemHead];
					pks = ITEM_PKS[itemHead];
					items = ITEM_ITEMS[itemHead];

					itemHead++;
					if (itemHead == ITEM_QUEUE_SIZE) {
						itemHead = 0;
					}
				}

				pos++; // 跳过\n

			} else if (ch == 'D') {
				// D|id:1:1|
				pos += 9;

				// 删除
				// pk = line[pos++] - '0';
				// while ((ch = line[pos++]) != '|') {
				// pk = (pk << 3) + (pk << 1) + (ch-'0');
				// }

				// 根据主键更新值
				// i = pos+delete_pk_off;
				// if(line[i]=='|' && line[i+1]=='N'){ // 防止误判
				// pk = 0;
				// for(i=delete_pk_off-1;i>=0;--i){
				// pk += vals[line[pos++]][i];
				// }
				// pos++;
				// }else{
				// bg = pos;
				pk = line[pos++] - '0';
				while ((ch = line[pos++]) != '|') {
					pk = (pk << 3) + (pk << 1) + (ch - '0');
				}
				// delete_pk_off = pos - 1 - bg;
				// }

				// 删除范围内的
				if (pk < END && pk > START) {
					ops[len] = op_delete;
					pks[len++] = pk;
					if (len == BATCH_SIZE) {
						len = 0;

						ITEM_FULL.release();
						ITEM_EMPTY.acquire();

						ops = ITEM_OPS[itemHead];
						old_pks = ITEM_OLD_PKS[itemHead];
						pks = ITEM_PKS[itemHead];
						items = ITEM_ITEMS[itemHead];

						itemHead++;
						if (itemHead == ITEM_QUEUE_SIZE) {
							itemHead = 0;
						}
					}
				}

				pos += 80; // 106;
				while (line[pos++] != '\n')
					;

			} else { // Insert

				// I|id:1:1|NULL| 1-7 bytes |
				pos += 14;

				item = 0;

				// 根据主键更新值
				if (line[pos + insert_pk_off] == '|') {
					pk = 0;
					for (i = insert_pk_off - 1; i >= 0; --i) {
						// System.out.println((char)line[pos]);
						pk += vals[line[pos++]][i];
					}
					pos++;
				} else {
					bg = pos;
					pk = line[pos++] - '0';
					while ((ch = line[pos++]) != '|') {
						pk = (pk << 3) + (pk << 1) + (ch - '0');
					}
					insert_pk_off = pos - 1 - bg;
				}

				// first_name:2:0|NULL| 3 bytes |
				pos += 20;
				name = ((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE;
				item |= (name << FIRST_NAME_OFF);
				pos += 23;

				// last_name:2:0|NULL| 3 or 6 bytes |
				name = ((line[pos + 1] << 5) ^ line[pos + 2]) & CHINESE_SIZE;
				item |= (name << LAST_NAME_1_OFF);

				if (line[pos + 3] == '|') {
					pos += 17;
				} else {
					name = ((line[pos + 4] << 5) ^ line[pos + 5]) & CHINESE_SIZE;
					item |= (name << LAST_NAME_2_OFF);
					pos += 20;
				}

				// sex:2:0|NULL|
				if (line[pos] == MAN_TAG) {
					item |= SEX_TAG;
				}

				// 3 bytes | score:1:0|NULL| 2-4 bytes |
				pos += 19;
				v = line[pos++] - '0';
				while ((ch = line[pos++]) != '|') {
					v = (v << 3) + (v << 1) + (ch - '0');
				}
				item |= v;

				// score2:1:0|NULL| 2-6 bytes |
				if (!Constants.IS_DEBUG) {
					pos += 16;
					v = line[pos++] - '0';
					while ((ch = line[pos++]) != '|') {
						v = (v << 3) + (v << 1) + (ch - '0');
					}
					item |= (v << SCORE2_OFF);
				}

				ops[len] = op_insert;
				pks[len] = pk;
				items[len++] = item;

				if (len == BATCH_SIZE) {
					len = 0;

					ITEM_FULL.release();
					ITEM_EMPTY.acquire();

					ops = ITEM_OPS[itemHead];
					old_pks = ITEM_OLD_PKS[itemHead];
					pks = ITEM_PKS[itemHead];
					items = ITEM_ITEMS[itemHead];

					itemHead++;
					if (itemHead == ITEM_QUEUE_SIZE) {
						itemHead = 0;
					}
				}

				pos++; // 跳过 \n
			}
		}
	}

	/// >> map 处理
	private static final int ITEM_QUEUE_SIZE = 100;
	private static final int BATCH_SIZE = 1024;

	private static final Semaphore ITEM_EMPTY = new Semaphore(ITEM_QUEUE_SIZE);
	private static final Semaphore ITEM_FULL = new Semaphore(0);

	private static final byte op_update = 0;
	private static final byte op_delete = 1;
	private static final byte op_insert = 2;
	private static final byte op_end = 3;

	private static final byte[][] ITEM_OPS = new byte[ITEM_QUEUE_SIZE][BATCH_SIZE];
	private static final int[][] ITEM_OLD_PKS = new int[ITEM_QUEUE_SIZE][BATCH_SIZE];
	private static final int[][] ITEM_PKS = new int[ITEM_QUEUE_SIZE][BATCH_SIZE];
	private static final long[][] ITEM_ITEMS = new long[ITEM_QUEUE_SIZE][BATCH_SIZE];

	private static long[] store() throws InterruptedException {

		final int HIGH_BIT = 8;
		int CAP = 10240000;// 8000000; 扩大一倍稍微慢了一点
		if (Constants.IS_DEBUG) {
			CAP = 10297446 + 1;
		}

		final long[] map1 = new long[CAP];
		final int SIZE = (1 << 14) - 1; // 2 << 12-1
		final IntLongHashMap[] map2 = new IntLongHashMap[SIZE + 1];
		// 1024 * 12 * 1024 * 16 = 192M
		for (int i = 0; i < map2.length; ++i) {
			map2[i] = new IntLongHashMap(1024);
		}
		final long NULL = 0;

		byte[] ops;
		int[] old_pks;
		int[] pks;
		long[] items;

		byte op;
		int old_pk, pk, i;
		long item, updateItem, part;
		int itemTail = 0;

		for (;;) {

			ITEM_FULL.acquire();
			ops = ITEM_OPS[itemTail];
			old_pks = ITEM_OLD_PKS[itemTail];
			pks = ITEM_PKS[itemTail];
			items = ITEM_ITEMS[itemTail];

			for (i = 0; i < BATCH_SIZE; ++i) {
				op = ops[i];
				if (op == op_update) {

					old_pk = old_pks[i];
					pk = pks[i];

					if (old_pk < CAP && old_pk > 0) {
						item = map1[old_pk];
						if (old_pk != pk) {
							map1[old_pk] = NULL;
						}
					} else {
						item = map2[(old_pk >>> HIGH_BIT) & SIZE].get(old_pk);
					}

					updateItem = items[i];

					part = updateItem & SCORE_TAG;
					if (part != 0) {
						item &= CLEAR_SCORE_TAG;
						item |= part;
					}

					part = updateItem & FIRST_NAME_TAG;
					if (part != 0) {
						item &= CLEAR_FIRST_NAME_TAG;
						item |= part;
					}

					part = updateItem & LAST_NAME_1_TAG;
					if (part != 0) {
						item &= CLEAR_LAST_NAME_TAG;
						item |= part;

						part = updateItem & LAST_NAME_2_TAG;
						if (part != 0) {
							item |= part;
						}
					}

					if (pk < CAP && pk > 0) {
						map1[pk] = item;
					} else {

						map2[(pk >>> HIGH_BIT) & SIZE].put(pk, item);
					}

				} else if (op == op_delete) {

					map1[pks[i]] = NULL;

				} else if (op == op_insert) {
					pk = pks[i];
					if (pk < CAP && pk > 0) {
						map1[pk] = items[i];
					} else {
						map2[(pk >>> HIGH_BIT) & SIZE].put(pk, items[i]);
					}

				} else {

					return map1;
				}
			}

			itemTail++;
			if (itemTail == ITEM_QUEUE_SIZE) {
				itemTail = 0;
			}
			ITEM_EMPTY.release();
		}
	}

	/// >> bit
	// 64位
	// | score2 | sex |lastName_2| lastName_1 | firstName | score
	// 19 1 10 10 10 14

	static final int SCORE2_OFF = 1 + 10 + 10 + 10 + 14;
	static final int SEX_OFF = 10 + 10 + 10 + 14;
	static final int LAST_NAME_2_OFF = 10 + 10 + 14;
	static final int LAST_NAME_1_OFF = 10 + 14;
	static final int FIRST_NAME_OFF = 14;
	static final int SCORE_OFF = 0;

	static final long SCORE_TAG = bits(0, FIRST_NAME_OFF);
	static final long FIRST_NAME_TAG = bits(FIRST_NAME_OFF, LAST_NAME_1_OFF);
	static final long LAST_NAME_1_TAG = bits(LAST_NAME_1_OFF, LAST_NAME_2_OFF);
	static final long LAST_NAME_2_TAG = bits(LAST_NAME_2_OFF, SEX_OFF);
	static final long SEX_TAG = bits(SEX_OFF, SCORE2_OFF);
	static final long SCORE2_TAG = 0xFFFFE00000000000L;

	static final long CLEAR_SCORE_TAG = ~SCORE_TAG;
	static final long CLEAR_LAST_NAME_TAG = ~(LAST_NAME_2_TAG | LAST_NAME_1_TAG);
	static final long CLEAR_FIRST_NAME_TAG = ~FIRST_NAME_TAG;

	private static final int CHINESE_SIZE = 1023;

	// [ , )
	private static final long bits(int from, int to) {
		return ((1L << to) - 1) ^ ((1L << from) - 1);
	}

}