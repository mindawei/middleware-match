package com.alibaba.middleware.race.sync;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.Socket;

public class QingClient {
	

	private String host;

	private int port;

	public QingClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	private final byte[] outs = new byte[38334025 + 1024];
	
	private volatile int outsLen = 0;
	private volatile boolean outsEnd = false;
	
	private final class WriteThread extends Thread{
		@Override
		public void run() {
			
			try{
			
				final String filePath = Constants.RESULT_HOME + File.separator + Constants.RESULT_FILE_NAME;
				final BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filePath));
				
				int pos = 0;
				int len = 0;
				while (true) {
					
					// 空转 wait
					while(pos == outsLen){
						if(outsEnd){
							out.close();
							System.exit(0);
						}
					}
					
					len = outsLen - pos;
					out.write(outs, pos, len);
					pos+=len;
				}
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	}
	
	private final byte[] bytes = new byte[38334025 + 1024];

	private final class ParseThread extends Thread{
		@Override
		public void run() {
			
			byte[] data;
			
			try{
			
				long item = 0;
				int last_name_2, pk = 0;
				int p = 0;

				int inPos = 0;	
				int outPos = 0;
				
				while (true) {
					
					// 空转 wait
					while(inPos+12 > reciveLen);
					
					// 主键 int		
					pk =  ( ((bytes[inPos]         ) << 24) |
					        ((bytes[inPos+1] & 0xff) << 16) |
					        ((bytes[inPos+2] & 0xff) <<  8) |
					        ((bytes[inPos+3] & 0xff))
					       );
					
					if(pk==0){
						outsEnd = true;
						return;
					}
			
					// 值 value
					item = (
							(((long) bytes[inPos + 4])        << 56) | 
							(((long) bytes[inPos + 5] & 0xff) << 48) | 
							(((long) bytes[inPos + 6] & 0xff) << 40) | 
							(((long) bytes[inPos + 7] & 0xff) << 32) | 
							(((long) bytes[inPos + 8] & 0xff) << 24) | 
							(((long) bytes[inPos + 9] & 0xff) << 16) | 
							(((long) bytes[inPos + 10] & 0xff) << 8 ) | 
							(((long) bytes[inPos + 11] & 0xff)      )
						   );
					
					inPos += 12;

					// id
					p = toByte(pk);
					System.arraycopy(nBytes, p, outs, outPos, INT_ML - p);
					outPos += INT_ML - p;
					outs[outPos++] = (byte) '\t';
							
					// first_name
					data = NAMES[(int) ((item & FIRST_NAME_TAG) >>> FIRST_NAME_OFF)];
					System.arraycopy(data, 0, outs, outPos, data.length);
					outPos += data.length;
					outs[outPos++] = (byte) '\t';

					// last_name
					data = NAMES[(int) ((item & LAST_NAME_1_TAG) >>> LAST_NAME_1_OFF)];
					System.arraycopy(data, 0, outs, outPos, data.length);
					outPos += data.length;

					if ((last_name_2 = (int) ((item & LAST_NAME_2_TAG) >>> LAST_NAME_2_OFF)) != 0) {
						data = NAMES[last_name_2];
						System.arraycopy(data, 0, outs, outPos, data.length);
						outPos += data.length;
					}
					outs[outPos++] = (byte) '\t';

					// sex:2:0
					if ((item & SEX_TAG) == 0) {
						data = WOMAN;
					} else {
						data = MAN;
					}
					System.arraycopy(data, 0, outs, outPos, data.length);
					outPos += data.length;
					outs[outPos++] = (byte) '\t';

					// score:1:0
					data = CHAR_NUM_CACHE[(short) (item & SCORE_TAG)];
					System.arraycopy(data, 0, outs, outPos, data.length);
					outPos += data.length;
					

//					if (!Constants.IS_DEBUG) {
					outs[outPos++] = (byte) '\t';
					
					// score2
					p = toByte((int)((item & SCORE2_TAG) >>> SCORE2_OFF));
					System.arraycopy(nBytes, p, outs, outPos, INT_ML - p);
					outPos += INT_ML - p;
					
//					}

					outs[outPos++] = (byte) '\n';
					
					outsLen = outPos;
					
				}
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	}
	
	private volatile int reciveLen = 0;
		
//	private volatile long t1 = 0;
	
	public void action() {
		
			Socket socket =null;
			
			
			while(true){
				try {
					socket = new Socket(host, port);
					break;
				} catch (Exception e) {
					try {
						Thread.sleep(200);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
	
			try {
						
				new ParseThread().start();
				new WriteThread().start();
				
				int len = -1;
				
				final BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
				int pos = 0;
				while ((len = in.read(bytes,pos,960)) != -1) {
//					if(t1==0){
//						 t1 = System.currentTimeMillis();
//					}
					pos+=len;
					reciveLen = pos;
				}
				
				bytes[pos] = 0;
				bytes[pos+1] = 0;
				bytes[pos+2] = 0;
				bytes[pos+3] = 0;
				pos+=12;
				reciveLen = pos;
				
				socket.close();
				return;
				
			} catch (Exception e) {
				e.printStackTrace();
				
			}
	}

	private static final int INT_ML = 8;
	private static final int INT_ML_P = INT_ML - 1;
	private static final byte[] nBytes = new byte[INT_ML];

	private static final int toByte(int pk) {
		int m = 0;
		int p = INT_ML_P;
		while (pk > 0) {
			m = pk % 10;
			pk /= 10;
			nBytes[p--] = (byte) (m + '0');
		}
		return ++p;
	}

	private static final int SCORE2_OFF = 1 + 10 + 10 + 10 + 14;
	private static final int SEX_OFF = 10 + 10 + 10 + 14;
	private static final int LAST_NAME_2_OFF = 10 + 10 + 14;
	private static final int LAST_NAME_1_OFF = 10 + 14;
	private static final int FIRST_NAME_OFF = 14;

	private static final long SCORE_TAG = bits(0, FIRST_NAME_OFF);
	private static final long FIRST_NAME_TAG = bits(FIRST_NAME_OFF, LAST_NAME_1_OFF);
	private static final long LAST_NAME_1_TAG = bits(LAST_NAME_1_OFF, LAST_NAME_2_OFF);
	private static final long LAST_NAME_2_TAG = bits(LAST_NAME_2_OFF, SEX_OFF);
	private static final long SEX_TAG = bits(SEX_OFF, SCORE2_OFF);

	private static final long SCORE2_TAG = 0xFFFFE00000000000L;

	// [ , )
	private static final long bits(int from, int to) {
		return ((1L << to) - 1) ^ ((1L << from) - 1);
	}

	// 初始化变量

	private static final byte[] MAN = { -25, -108, -73 };
	private static final byte[] WOMAN = { -27, -91, -77 };

	// 10位
	private static final int NAMES_SIZE = 1023;
	// 字母表
	private static final byte[][] NAMES = new byte[NAMES_SIZE + 1][];

	// 延迟加载
	private static final int CHAR_NUM_CACHE_SIZE = 8500;

	// 将8500内的数字的对应的字节缓存起来，加快传输时转换速度
	private static final byte[][] CHAR_NUM_CACHE = new byte[CHAR_NUM_CACHE_SIZE][];

	static {
		final String[] names = "郑,静,依,李,莉,孙,景,励,侯,闵,阮,田,丙,钱,邹,名,甜,柳,明,周,力,晶,杨,铭,徐,兲,五,军,敏,吴,江,天,恬,黎,刘,三,彭,城,丁,立,京,陈,诚,王,六,成,民,赵,益,乙,俊,刚,林,九,甲,十,高,骏,君,发,八,七,二,四,他,一,雨,我,人,乐,上,娥"
				.split(",");
		long index;
		byte[] bytes;
		for (String name : names) {
			bytes = name.getBytes();
			index = ((bytes[1] << 5) ^ bytes[2]) & NAMES_SIZE;
			NAMES[(int) index] = bytes;
		}

		for (int i = 0; i < CHAR_NUM_CACHE_SIZE; i++) {
			CHAR_NUM_CACHE[i] = ("" + i).getBytes();
		}
	}

	// 722 -> 408
	// 9082741 -> 4427400
	
}