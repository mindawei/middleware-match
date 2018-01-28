package com.alibaba.middleware.race.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Constructor {

	public static boolean isFinished = false;

	// 标记
	static class FileInfo {
		String tag;
		String fileName;

		public FileInfo(String tag, String fileName) {
			super();
			this.tag = tag;
			this.fileName = fileName;
		}
	}

	private static final String orderTag = "o";
	private static final String buyerTag = "b";
	private static final String goodTag = "g";

	/**
	 * 保持 disk1 disk2 disk3 的序列
	 */
	private static List<FileInfo> sortDataFiles(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles) {

		List<FileInfo> fileInfos = new ArrayList<FileInfo>();

		List<FileInfo> disk1Files = new ArrayList<FileInfo>();
		List<FileInfo> disk2Files = new ArrayList<FileInfo>();
		List<FileInfo> disk3Files = new ArrayList<FileInfo>();

		// 归类
		for (String orderFile : orderFiles) {
			if (orderFile.startsWith("/disk1")) {
				disk1Files.add(new FileInfo(orderTag, orderFile));
			} else if (orderFile.startsWith("/disk2")) {
				disk2Files.add(new FileInfo(orderTag, orderFile));
			} else {
				disk3Files.add(new FileInfo(orderTag, orderFile));
			}
		}

		for (String buyerFile : buyerFiles) {
			if (buyerFile.startsWith("/disk1")) {
				disk1Files.add(new FileInfo(buyerTag, buyerFile));
			} else if (buyerFile.startsWith("/disk2")) {
				disk2Files.add(new FileInfo(buyerTag, buyerFile));
			} else {
				disk3Files.add(new FileInfo(buyerTag, buyerFile));
			}
		}

		for (String goodFile : goodFiles) {
			if (goodFile.startsWith("/disk1")) {
				disk1Files.add(new FileInfo(goodTag, goodFile));
			} else if (goodFile.startsWith("/disk2")) {
				disk2Files.add(new FileInfo(goodTag, goodFile));
			} else {
				disk3Files.add(new FileInfo(goodTag, goodFile));
			}
		}

		// 冲突保持在1/3
		fileInfos.addAll(disk1Files);
		fileInfos.addAll(disk2Files);
		fileInfos.addAll(disk3Files);

		// 划分
		// int index =0;
		// boolean needToDivide = true;
		// while(needToDivide){
		// needToDivide = false;
		// if(index<disk1Files.size()){
		// fileInfos.add(disk1Files.get(index));
		// needToDivide = true;
		// }
		// if(index<disk2Files.size()){
		// fileInfos.add(disk2Files.get(index));
		// needToDivide = true;
		// }
		// if(index<disk3Files.size()){
		// fileInfos.add(disk3Files.get(index));
		// needToDivide = true;
		// }
		// index++;
		// }
		//
		return fileInfos;
	}

	/**
	 * 构建
	 */
	public static void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {

		final FileMapper fileMapper = FileMapper.getInstance();
		final DataWriter dataWriter = new DataWriter();
		final ConcurrentSkipListSet<String> orderKeys = new ConcurrentSkipListSet<String>();
		final ConcurrentSkipListSet<String> buyerKeys = new ConcurrentSkipListSet<String>();
		final ConcurrentSkipListSet<String> goodKeys = new ConcurrentSkipListSet<String>();

		final int orderThreadNum = orderFiles.size();
		final List<FileInfo> fileInfos = sortDataFiles(orderFiles, buyerFiles,
				goodFiles);

		// 分线程执行,3个读线程
		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);

		// 文件数 +　２个写进程
		final int semaphoreNum = fileInfos.size() + 2;
		final Semaphore semaphore = new Semaphore(semaphoreNum);

		semaphore.drainPermits();

		final long tBegin = System.currentTimeMillis();

		fileMapper.createDirs(fileInfos,storeFolders);
		System.out.println("基本目录创建完毕！");

		final AtomicInteger endFileNum = new AtomicInteger();
		final AtomicInteger endOrderFileNum = new AtomicInteger();

		for (int i=0;i<fileInfos.size();++i) {
			
			final FileInfo fileInfo  =  fileInfos.get(i);
			
			final String fileIndex = Integer.toString(i);

			if (fileInfo.tag.equals(orderTag)) {

				fixedThreadPool.execute(new Runnable() {
					public void run() {
						try {

							System.out.println("order deal " + fileInfo.fileName);

							new DataFileHandler() {

								@Override
								void handleRows(List<String> lines) {

									List<Index> lsIndex = new ArrayList<Index>(lines.size());
									
									List<Info> lsInfo = new ArrayList<Info>(lines.size() * 3);

									for (String line : lines) {

										Map<String, String> kvMap = createKVMapFromLine(line);

										// 保存key
										orderKeys.addAll(kvMap.keySet());

										String orderid = kvMap
												.get(KeyUtil.ORDER_ID);
										String buyerid = kvMap
												.get(KeyUtil.BUYER_ID);
										String goodid = kvMap
												.get(KeyUtil.GOOD_ID);
										String createtime = kvMap
												.get(KeyUtil.CREATE_TIME);

										long _orderid = Long.parseLong(orderid);

										// 索引
										String indexFileName = fileMapper
												.getOrderDataIndexPath(_orderid);
										String key = orderid;
										String dataFileName = fileIndex;
										long pos = filePostion;
										int len = line.getBytes().length;
										lsIndex.add(new Index(indexFileName,
												key, dataFileName, pos, len));
										filePostion += len + 1; // 文字内容 +　换行符

										// 买家信息添加时间
										lsInfo.add(new Info(fileMapper.getOrerInfoPath(_orderid),orderid, buyerid+","+goodid));
										
										lsInfo.add(new Info(fileMapper.getBuyerInfoPath(buyerid),buyerid, createtime + ","+ orderid));

										lsInfo.add(new Info(fileMapper.getGoodInfoPath(goodid),goodid, orderid));
										
										

									}

									dataWriter.addAllIndex(lsIndex);
									dataWriter.addAllInfo(lsInfo);

								}

							}.handle(fileInfo.fileName);

							long tEnd = System.currentTimeMillis();
							System.out.println(fileInfo.fileName + " cost:"
									+ (tEnd - tBegin));

						} catch (IOException e) {
							e.printStackTrace();
						} finally {

							endOrderFileNum.incrementAndGet();
							endFileNum.incrementAndGet();
							semaphore.release();
						}
					}
				});

			} else if (fileInfo.tag.equals(buyerTag)) {

				fixedThreadPool.execute(new Runnable() {
					public void run() {

						try {
							System.out.println("buyer deal " + fileInfo.fileName);
							new DataFileHandler() {
								@Override
								void handleRows(List<String> lines) {

									List<Index> lsIndex = new ArrayList<Index>(
											lines.size());

									for (String line : lines) {

										Map<String, String> kvMap = createKVMapFromLine(line);

										// 保存key
										buyerKeys.addAll(kvMap.keySet());

										String buyerid = kvMap
												.get(KeyUtil.BUYER_ID);

										// 索引
										String indexFileName = fileMapper
												.getBuyerDataIndexPath(buyerid);
										String key = buyerid;
										String dataFileName = fileIndex;
										long pos = filePostion;
										int len = line.getBytes().length;
										lsIndex.add(new Index(indexFileName,
												key, dataFileName, pos, len));
										filePostion += len + 1; // 文字内容 +　换行符

									}

									dataWriter.addAllIndex(lsIndex);

								}

							}.handle(fileInfo.fileName);

							long tEnd = System.currentTimeMillis();
							System.out.println(fileInfo.fileName + " cost:"
									+ (tEnd - tBegin));

						} catch (IOException e) {
							e.printStackTrace();
						} finally {

							endFileNum.incrementAndGet();
							semaphore.release();
						}

					}
				});

			} else {
				fixedThreadPool.execute(new Runnable() {
					public void run() {

						try {

							System.out.println("good deal " + fileInfo.fileName);

							new DataFileHandler() {

								@Override
								void handleRows(List<String> lines) {

									List<Index> lsIndex = new ArrayList<Index>(
											lines.size());

									for (String line : lines) {

										Map<String, String> kvMap = createKVMapFromLine(line);

										// 保存key
										goodKeys.addAll(kvMap.keySet());

										String goodid = kvMap
												.get(KeyUtil.GOOD_ID);

										// 索引
										String indexFileName = fileMapper
												.getGoodDataIndexPath(goodid);
										String key = goodid;
										String dataFileName = fileIndex;
										long pos = filePostion;
										int len = line.getBytes().length;
										lsIndex.add(new Index(indexFileName,
												key, dataFileName, pos, len));
										filePostion += len + 1; // 文字内容 +　换行符

									}

									dataWriter.addAllIndex(lsIndex);

								}

							}.handle(fileInfo.fileName);

							long tEnd = System.currentTimeMillis();
							System.out.println(fileInfo.fileName + " cost:"
									+ (tEnd - tBegin));

						} catch (IOException e) {
							e.printStackTrace();
						} finally {

							endFileNum.incrementAndGet();

							semaphore.release();

						}

					}
				});
			}

		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("write index begin");
				while (endFileNum.get() < fileInfos.size()) {
					if (dataWriter.isIndexsNeedOutput())
						dataWriter.writeIndexToFile();
					else
						Thread.yield();
				}

				dataWriter.writeIndexToFile();
				System.out.println("write index end");
				semaphore.release();
			}
		}).start();

		new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("write info begin");
				while (endOrderFileNum.get() < orderThreadNum) {
					if (dataWriter.isInfoNeedOutput())
						dataWriter.writInfoToFile();
					else
						Thread.yield();
				}

				dataWriter.writInfoToFile();

				System.out.println("write info end!");
				semaphore.release();

			}
		}).start();

		// 监听全部结束信号
		new Thread(new Runnable() {
			@Override
			public void run() {

				try {
					semaphore.acquire(semaphoreNum);
					semaphore.release(semaphoreNum);

					// 初始化key
					KeyUtil.getInstance().init(orderKeys, buyerKeys, goodKeys);

					long tEnd = System.currentTimeMillis();

					System.out.println("all time cost:" + (tEnd - tBegin));
//					System.out.println("maxInfoLineSize:"
//							+ DataReader.getInstance().maxInfoLineSize);
					// System.out.println("order num:"+statistic.orderNum.get());
					// System.out.println("buyer num:"+statistic.buyerNum.get());
					// System.out.println("good num:"+statistic.goodNum.get());
					// System.out.println("orderStartTime:"+statistic.orderStartTime);
					// System.out.println("orderEndTime:"+statistic.orderEndTime);

					// writeFixedThreadPool.shutdown();
					fixedThreadPool.shutdown();

					isFinished = true;

				} catch (InterruptedException e) {
				}
			}
		}).start();

		//Thread.sleep(3000000L);
		Thread.sleep(3595000L);
		System.out.println("return now");

	}

	static abstract class DataFileHandler {

		protected long filePostion = 0;

		abstract void handleRows(List<String> lines);

		public void handle(String fileName) throws IOException {

			int BATCH_SIZE = 256;

			List<String> lines = new ArrayList<>(BATCH_SIZE);

			try {

				int readBufferSize = 4 * 1024 * 1024; // 4M

				File fin = new File(fileName);
				RandomAccessFile randomAccessFile = new RandomAccessFile(fin,
						"r");
				int bufSize = (int) Math.min(readBufferSize,
						randomAccessFile.length());

				FileChannel fcin = randomAccessFile.getChannel();
				ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
				byte[] bs = new byte[bufSize];

				ByteBuffer preBytes = ByteBuffer.allocate(70000);

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
							if (lines.size() == BATCH_SIZE) {
								handleRows(lines);
								lines.clear();
							}
							lines.add(line);

						} else {
							preBytes.put(bs[i]);
						}
					}

				}

				handleRows(lines);
				lines.clear();

				randomAccessFile.close();
			} catch (IOException e) {
			}

		}

		protected Map<String, String> createKVMapFromLine(String line) {
			String[] kvs = line.split("\t");
			Map<String, String> kvMap = new HashMap<String, String>();
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				kvMap.put(key, value);
			}
			return kvMap;
		}
	}

}
