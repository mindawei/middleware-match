package com.alibaba.middleware.race.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.file.Query.SearchInfo;

public class DataReader {
	
	private static DataReader instance = new DataReader();
	
	private DataReader(){}
	
	public static DataReader getInstance(){
		return instance;
	}

	private FileMapper fileMapper = FileMapper.getInstance();
	

	public int maxIndexLineSize = 4000;
	
	public int maxInfoLineSize = 4000;

	private final int INFO_READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4M

	private final int INDEX_DATA_READ_BUFFER_SIZE = 1024 * 1024; // 1M

	/** 查买家有哪些订单 */
	public List<BuyerOrderInfo> readBuyerInfo(String buyerid) {

		// 存放“买家有哪些订单”的文件地址
		String fileName = fileMapper.getBuyerInfoPath(buyerid);
		List<BuyerOrderInfo> values = new LinkedList<BuyerOrderInfo>();

		try {

			File fin = new File(fileName);
			RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
			int bufSize = INFO_READ_BUFFER_SIZE;

			FileChannel fcin = randomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			ByteBuffer preBytes = ByteBuffer.allocate(maxInfoLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);
						// line 为当前行

						String[] kv = line.split(",");

						if (kv[0].equals(buyerid)) {

							for (int ii = 1; ii < kv.length; ii += 2) {

								long createtime = Long.parseLong(kv[ii]);
								long orderId = Long.parseLong(kv[ii + 1]);

								values.add(new BuyerOrderInfo(orderId,
										createtime));

							}
						}

					} else {
						preBytes.put(bs[i]);
					}
				}

			}

			randomAccessFile.close();
		} catch (IOException e) {
		}

		return values;
	}

	/** 查商品有哪些订单 */
	public List<Long> readGoodInfo(String goodid) {

		// 存放“商品有哪些订单”的文件地址
		String fileName = fileMapper.getGoodInfoPath(goodid);

		List<Long> values = new LinkedList<Long>();

		try {

			File fin = new File(fileName);
			RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
			int bufSize = INFO_READ_BUFFER_SIZE;

			FileChannel fcin = randomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			ByteBuffer preBytes = ByteBuffer.allocate(maxInfoLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);

						// line 为当前行

						String[] kv = line.split(",");
						if (kv[0].equals(goodid)) {
							for (int ii = 1; ii < kv.length; ++ii) {
								values.add(Long.parseLong(kv[ii]));
							}
						}

					} else {
						preBytes.put(bs[i]);
					}
				}

			}

			randomAccessFile.close();
		} catch (IOException e) {
		}

		return values;
	}

	
	public List<String> readOrderInfo(long orderId) {
		// 存放“商品有哪些订单”的文件地址
		String fileName = fileMapper.getOrerInfoPath(orderId);
		String srtOrderId = String.valueOf(orderId);

		List<String> values = new ArrayList<String>(2);

		try {

			File fin = new File(fileName);
			RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
			int bufSize = INFO_READ_BUFFER_SIZE;

			FileChannel fcin = randomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			ByteBuffer preBytes = ByteBuffer.allocate(maxInfoLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);

						// line 为当前行

						String[] kv = line.split(",");
						
						if (kv[0].equals(srtOrderId)) {
							values.add(kv[1]);
							values.add(kv[2]);
							
							// 找到返回
							randomAccessFile.close();
							return values;
						}

					} else {
						preBytes.put(bs[i]);
					}
				}

			}

			randomAccessFile.close();
		} catch (IOException e) {
		}

		return values;
		
		
	}
	
	private Map<String, String> getDataMap(String fileName, long pos, int len) {

		try {

			RandomAccessFile raf = new RandomAccessFile(new File(fileName), "r");
			raf.seek(pos);
			byte[] buf = new byte[len];
			raf.read(buf);
			raf.close();

			String[] kvs = new String(buf).split("\t");
			Map<String, String> kvMap = new HashMap<String, String>();
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				kvMap.put(rawkv.substring(0, p), rawkv.substring(p + 1));
			}
			return kvMap;

		} catch (Exception e) {
			return null;
		}

	}

	/** 读信息 */
	public Map<String, String> readData(String indexfileName,String dataId) {

		Map<String, String> result = null;

		try {
			
			RandomAccessFile indexRandomAccessFile = new RandomAccessFile(new File(indexfileName), "r");
			int bufSize = (int)Math.min(indexRandomAccessFile.length(), INDEX_DATA_READ_BUFFER_SIZE);

			FileChannel fcin = indexRandomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			boolean isEnd = false;

			ByteBuffer preBytes = ByteBuffer.allocate(maxIndexLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);

						// line 为当前行
						String[] items = line.split(",");
						// 找到索引关键字了
						if (dataId.equals(items[0])) {
							String fileName = fileMapper.getFileNameByIndex(items[1]);
							long pos = Long.parseLong(items[2]);
							int len = Integer.parseInt(items[3]);
							result = getDataMap(fileName, pos, len);
							isEnd = true;
							break;
						}

					} else {
						preBytes.put(bs[i]);
					}
				}

				if (isEnd)
					break;

			}

			indexRandomAccessFile.close();

		} catch (IOException e) {
		}

		return result;
	}

	/** 判断id是否存在 */
	public boolean isOrderIdExist(long orderId) {

		String indexFileName = fileMapper.getOrderDataIndexPath(orderId);
		String strOrderId = String.valueOf(orderId);
		
		try {

			RandomAccessFile indexRandomAccessFile = new RandomAccessFile(
					new File(indexFileName), "r");
			int bufSize = INDEX_DATA_READ_BUFFER_SIZE;

			FileChannel fcin = indexRandomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			ByteBuffer preBytes = ByteBuffer.allocate(maxIndexLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);

						// line 为当前行
						String[] items = line.split(",");
						// 找到索引关键字了
						if (strOrderId.equals(items[0])) {
							indexRandomAccessFile.close();
							return true;
						}
					} else {
						preBytes.put(bs[i]);
					}
				}
			}
			indexRandomAccessFile.close();
		} catch (IOException e) {
		}

		return false;
	}

	public void getFileNamePosLen(String indexFileName, List<SearchInfo> infos) {
		
		int leftNum = infos.size();
		
		try {

			RandomAccessFile indexRandomAccessFile = new RandomAccessFile(
					new File(indexFileName), "r");
			int bufSize = (int)Math.min(indexRandomAccessFile.length(), INDEX_DATA_READ_BUFFER_SIZE);

			FileChannel fcin = indexRandomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			ByteBuffer preBytes = ByteBuffer.allocate(maxIndexLineSize);
			while (fcin.read(rBuffer) != -1) {

				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();

				for (int i = 0; i < rSize; ++i) {

					if (bs[i] == '\n') {

						int size = preBytes.position();
						byte[] lineBytes = new byte[size];
						// 拷贝
						preBytes.flip();
						preBytes.get(lineBytes);
						preBytes.clear();
						// 转换
						String line = new String(lineBytes);

						// line 为当前行
						String[] items = line.split(",");
						long orderId = Long.parseLong(items[0]);
								
						// 找到索引关键字了
						for(SearchInfo info :infos){
							if(info.orderId==orderId){
								info.fileName = fileMapper.getFileNameByIndex(items[1]);;
								info.pos = Long.parseLong(items[2]);
								info.len = Integer.parseInt(items[3]);
								leftNum--;
								break;
							}
						}

					} else {
						preBytes.put(bs[i]);
					}
					
					if (leftNum==0)
						break;
					
				}

				if (leftNum==0)
					break;

			}

			indexRandomAccessFile.close();

		} catch (IOException e) {
		}
		
	}

	public void getOrderDatas(String fileName, List<SearchInfo> infos) {
		
		// 按pos从小到大排序
		Collections.sort(infos, new Comparator<SearchInfo>() {
			@Override
			public int compare(SearchInfo s1, SearchInfo s2) {
				if (s1.pos < s2.pos)
					return -1;
				else if (s1.pos > s2.pos)
					return 1;
				else
					return 0;
			}
		});

		try {

			RandomAccessFile raf = new RandomAccessFile(new File(fileName), "r");

			for (SearchInfo info : infos) {

				raf.seek(info.pos);
				byte[] buf = new byte[info.len];
				raf.read(buf);
				
				String[] kvs = new String(buf).split("\t");
				Map<String, String> kvMap = new HashMap<String, String>();
				for (String rawkv : kvs) {
					int p = rawkv.indexOf(':');
					kvMap.put(rawkv.substring(0, p), rawkv.substring(p + 1));
				}
				info.kvMap = kvMap;

			}

			raf.close();
		} catch (Exception e) {

		}

	}

	

}
