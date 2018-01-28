//package com.alibaba.middleware.race.utils.data;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.ByteBuffer;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.channels.FileChannel.MapMode;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentSkipListSet;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Semaphore;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import com.alibaba.middleware.race.Statistic;
//import com.alibaba.middleware.race.file.FileMapper;
//
//public class Constructor {
//	
//	public static boolean isFinished = false;
//
//	/**
//	 * 构建
//	 */
//	public static void construct(Collection<String> orderFiles, Collection<String> buyerFiles,
//			Collection<String> goodFiles, Collection<String> storeFolders) throws IOException, InterruptedException {
//
//		final FileMapper fileMapper = FileMapper.getInstance();
//		final DataWriter dataWriter = new DataWriter();
//		final ConcurrentSkipListSet<String> orderKeys = new ConcurrentSkipListSet<String>();
//		final ConcurrentSkipListSet<String> buyerKeys = new ConcurrentSkipListSet<String>();
//		final ConcurrentSkipListSet<String> goodKeys = new ConcurrentSkipListSet<String>();
//		final Statistic statistic = Statistic.getInstance();
//
//		// 标记
//		class FileInfo {
//			
//			String tag;
//			String fileName;
//
//			public FileInfo(String tag, String fileName) {
//				super();
//				this.tag = tag;
//				this.fileName = fileName;
//			}
//		}
//
//		final String orderTag = "o";
//		final String buyerTag = "b";
//		final String goodTag = "g";
//
//		final List<FileInfo> fileInfos = new ArrayList<FileInfo>();
//
//		final int orderThreadNum = orderFiles.size();
//		for (String orderFile : orderFiles) {
//			fileInfos.add(new FileInfo(orderTag, orderFile));
//		}
//
//		for (String buyerFile : buyerFiles) {
//			fileInfos.add(new FileInfo(buyerTag, buyerFile));
//		}
//
//		for (String goodFile : goodFiles) {
//			fileInfos.add(new FileInfo(goodTag, goodFile));
//		}
//
//		// 分线程执行
//		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(2);
//
//		final int semaphoreNum = fileInfos.size()+2;
//		final Semaphore semaphore = new Semaphore(semaphoreNum);
//
//		semaphore.drainPermits();
//
//		final long tBegin = System.currentTimeMillis();
//
//		fileMapper.createDirs(storeFolders);
//		System.out.println("基本目录创建完毕！");
//
//		
//		final AtomicInteger endFileNum = new AtomicInteger();
//		final AtomicInteger endOrderFileNum = new AtomicInteger();
//		
//
//		for (FileInfo fileInfo : fileInfos) {
//
//			final List<String> files = new ArrayList<>();
//			files.add(fileInfo.fileName);
//
//			if (fileInfo.tag.equals(orderTag)) {
//
//				fixedThreadPool.execute(new Runnable() {
//					public void run() {
//						try {
//
//							System.out.println("order deal " + files.get(0));
//							
//							new DataFileHandler() {
//
//								@Override
//								void handleRows(List<String> lines) {
//
//									List<Data> lsData = new ArrayList<Data>(lines.size());
//									List<Info> lsInfo = new ArrayList<Info>(lines.size()*2);
//									
//									for (String line : lines) {
//
//										Map<String, String> kvMap = createKVMapFromLine(line);
//										
//										// 保存key
//										orderKeys.addAll(kvMap.keySet());
//										
//										String orderid = kvMap.get(KeyUtil.ORDER_ID);
//										String buyerid = kvMap.get(KeyUtil.BUYER_ID);
//										String goodid = kvMap.get(KeyUtil.GOOD_ID);
//										String createtime = kvMap.get(KeyUtil.CREATE_TIME);
//										
//									
//										lsData.add(new Data(orderid,fileMapper.getOrderDataPath(Long.parseLong(orderid)), line));
//										
//										// 买家信息添加时间
//										lsInfo.add(new Info(fileMapper.getBuyerInfoPath(buyerid),buyerid, createtime+","+orderid));
//										
//										lsInfo.add(new Info(fileMapper.getGoodInfoPath(goodid), goodid , orderid));
//
//										
//										// 统计
//										//statistic.updateTime(Long.parseLong(createtime));
//										//statistic.orderNum.incrementAndGet();
//										
//									}
//
//									dataWriter.addAllData(lsData);
//
//									dataWriter.addAllInfo(lsInfo);
//
//									
//								}
//
//
//							}.handle(files);
//
//							long tEnd = System.currentTimeMillis();
//							System.out.println(files.get(0) + " cost:" + (tEnd - tBegin));
//
//						} catch (IOException e) {
//							e.printStackTrace();
//						} finally {
//
//							endOrderFileNum.incrementAndGet();
//							endFileNum.incrementAndGet();
//							semaphore.release();
//						}
//					}
//				});
//
//			} else if (fileInfo.tag.equals(buyerTag)) {
//
//				fixedThreadPool.execute(new Runnable() {
//					public void run() {
//
//						try {
//							System.out.println("buyer deal " + files.get(0));
//							new DataFileHandler() {
//								@Override
//								void handleRows(List<String> lines) {
//
//									List<Data> lsData = new ArrayList<Data>(lines.size());
//								
//									for (String line : lines) {
//
//										Map<String, String> kvMap = createKVMapFromLine(line);
//										
//										// 保存key
//										buyerKeys.addAll(kvMap.keySet());
//
//								
//										String buyerid = kvMap.get(KeyUtil.BUYER_ID);
//										String fileName = fileMapper.getBuyerDataPath(buyerid);
//
//										// 将buyerid加入布隆过滤器
//										// BloomFilterManager.BUYER_FILTER.add(new Key(buyerid.getBytes()));
//									
//										lsData.add(new Data(buyerid,fileName, line));
//										
//										//statistic.buyerNum.incrementAndGet();
//
//									}
//
//									dataWriter.addAllData(lsData);
//
//								}
//
//								
//							}.handle(files);
//
//							long tEnd = System.currentTimeMillis();
//							System.out.println(files.get(0) + " cost:" + (tEnd - tBegin));
//
//						} catch (IOException e) {
//							e.printStackTrace();
//						} finally {
//							
//							endFileNum.incrementAndGet();
//							semaphore.release();
//						}
//
//					}
//				});
//
//			} else {
//				fixedThreadPool.execute(new Runnable() {
//					public void run() {
//
//						try {
//
//							System.out.println("good deal " + files.get(0));
//
//							new DataFileHandler() {
//								
//								@Override
//								void handleRows(List<String> lines) {
//
//									List<Data> lsData = new ArrayList<Data>(lines.size());
//								
//									for (String line : lines) {
//
//										Map<String, String> kvMap = createKVMapFromLine(line);
//									
//										// 保存key
//										goodKeys.addAll(kvMap.keySet());
//
//										String goodid = kvMap.get(KeyUtil.GOOD_ID);
//										String fileName = fileMapper.getGoodDataPath(goodid);
//										
//										// 将ID加入布隆过滤器
//										// BloomFilterManager.GOOD_FILTER.add(new Key(goodid.getBytes()));
//										
//										
//										lsData.add(new Data(goodid,fileName, line));
//										
//										//statistic.goodNum.incrementAndGet();
//
//									}
//
//				
//									dataWriter.addAllData(lsData);
//
//								}
//
//							
//								
//								
//							}.handle(files);
//
//							long tEnd = System.currentTimeMillis();
//							System.out.println(files.get(0) + " cost:" + (tEnd - tBegin));
//
//						} catch (IOException e) {
//							e.printStackTrace();
//						} finally {
//							
//							endFileNum.incrementAndGet();
//							
//							semaphore.release();
//						
//						}
//
//					}
//				});
//			}
//
//		}
//		
//		
//		// 分线程执行
////	   ExecutorService writeFixedThreadPool = Executors.newFixedThreadPool(2);
//
//
//	  new Thread(new Runnable() {
//			@Override
//			public void run() {
//				System.out.println("write1 begin");
//				while (endFileNum.get()<fileInfos.size()) {
//					
//					dataWriter.writeDataToFile();
//					
//				}
//				dataWriter.writeDataToFile();
//
//				
//				System.out.println("write1 end");
//				semaphore.release();
//				
//			}
//		}).start();
//	   	   
//	  new Thread(new Runnable() {
//			@Override
//			public void run() {
//				System.out.println("write2 begin");
//				while (endOrderFileNum.get()<orderThreadNum) {
//					dataWriter.writInfoToFile();
//				}
//				
//				dataWriter.writInfoToFile();
//				
//				System.out.println("write2 end!");
//				semaphore.release();
//				
//			}
//		}).start();
//		
//		
//	  // 监听全部结束信号
//	  new Thread(new Runnable() {
//		  @Override
//			public void run() {
//			  
//			  	try {
//					semaphore.acquire(semaphoreNum);
//				semaphore.release(semaphoreNum);
//				
//				// 初始化key 
//				KeyUtil.getInstance().init(orderKeys, buyerKeys, goodKeys);
//				
//				long tEnd = System.currentTimeMillis();
//				
//				System.out.println("all time cost:" + (tEnd - tBegin));
//				System.out.println("maxInfoLineSize:"+DataWriter.maxInfoLineSize);
////				System.out.println("order num:"+statistic.orderNum.get());
////				System.out.println("buyer num:"+statistic.buyerNum.get());
////				System.out.println("good num:"+statistic.goodNum.get());
////				System.out.println("orderStartTime:"+statistic.orderStartTime);
////				System.out.println("orderEndTime:"+statistic.orderEndTime);
//				
//				//writeFixedThreadPool.shutdown();
//				fixedThreadPool.shutdown();
//				
//				isFinished = true;
//		  
//				
//				} catch (InterruptedException e) {
//				}
//		}
//	  }).start();
//		
//		
//	  //Thread.sleep(3000000L);
//	  Thread.sleep(3590000L);
//	  System.out.println("return now");
//	  
//
//	}
//
//	
//		
//	static abstract class DataFileHandler {
//
//		abstract void handleRows(List<String> lines);
//
//		public void handle(Collection<String> files) throws IOException {
//			
//			for (String file : files) {
//				
//				int BATCH_SIZE = 20;
//
//				List<String> lines = new ArrayList<>(BATCH_SIZE);
//				
//				try {
//
//					File fin = new File(file);
//					RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
//					int bufSize = (int)Math.min(Parameters.getInstance().readBufferSize, randomAccessFile.length());
//					
//					FileChannel fcin = randomAccessFile.getChannel();
//					ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
//					byte[] bs = new byte[bufSize];
//					
//
//					ByteBuffer preBytes = ByteBuffer.allocate(70000);
//					
//					long fileLength = fin.length();
//					
//					MappedByteBuffer inputBuffer = fcin.map(MapMode.READ_ONLY, 0, fileLength);
//					
//					
//					//while (fcin.read(rBuffer) != -1) {
//					
//						
//					for(int offset =0;offset<fileLength;offset+=bufSize){
//						
//						int rSize;
//						
//						if(fileLength-offset>bufSize){
//							inputBuffer.get(bs);
//							rSize = bufSize;
//						}else{
//						  for (int i = 0; i < fileLength - offset; i++)
//							 bs[i] = inputBuffer.get();
//						  rSize = (int)(fileLength - offset);
//						}
//						
//						// 将缓存的拷贝到数组中						
//						for(int i=0;i<rSize;++i){
//							
//							if(bs[i]=='\n'){
//								
//								int size = preBytes.position();
//								
//								byte[] lineBytes = new byte[size];
//								// 拷贝
//								preBytes.flip();
//								preBytes.get(lineBytes);
//								preBytes.clear();
//							
//								// 转换
//								String line = new String(lineBytes);
//								if (lines.size() == BATCH_SIZE) {
//									handleRows(lines);
//									lines.clear();
//								}
//								lines.add(line);
//					
//							}else{
//								preBytes.put(bs[i]);
//							}
//						}
//			
//					}
//					
//					handleRows(lines);
//					lines.clear();
//					
//					randomAccessFile.close();
//				} catch (IOException e) {}
//				
//			}
//
////		public void handle(Collection<String> files) throws IOException {
////			
////			for (String file : files) {
////				
////				int BATCH_SIZE = 20;
////
////				List<String> lines = new ArrayList<>(BATCH_SIZE);
////				
////				try {
////
////					File fin = new File(file);
////					RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
////					int bufSize = (int)Math.min(Parameters.getInstance().readBufferSize, randomAccessFile.length());
////					
////					FileChannel fcin = randomAccessFile.getChannel();
////					ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
////					byte[] bs = new byte[bufSize];
////
////					ByteBuffer preBytes = ByteBuffer.allocate(70000);
////					
////					while (fcin.read(rBuffer) != -1) {
////						
////						// 将缓存的拷贝到数组中
////						int rSize = rBuffer.position();
////						rBuffer.rewind();
////						rBuffer.get(bs);
////						rBuffer.clear();
////						
////						for(int i=0;i<rSize;++i){
////							
////							if(bs[i]=='\n'){
////								
////								int size = preBytes.position();
////								
////								byte[] lineBytes = new byte[size];
////								// 拷贝
////								preBytes.flip();
////								preBytes.get(lineBytes);
////								preBytes.clear();
////							
////								// 转换
////								String line = new String(lineBytes);
////								if (lines.size() == BATCH_SIZE) {
////									handleRows(lines);
////									lines.clear();
////								}
////								lines.add(line);
////					
////							}else{
////								preBytes.put(bs[i]);
////							}
////						}
////			
////					}
////					
////					handleRows(lines);
////					lines.clear();
////					
////					randomAccessFile.close();
////				} catch (IOException e) {}
////				
////			}
//		}
//
//		protected Map<String, String> createKVMapFromLine(String line) {
//			String[] kvs = line.split("\t");
//			Map<String, String> kvMap = new HashMap<String, String>();
//			for (String rawkv : kvs) {
//				int p = rawkv.indexOf(':');
//				String key = rawkv.substring(0, p);
//				String value = rawkv.substring(p + 1);
//				kvMap.put(key, value);
//			}
//			return kvMap;
//		}
//	}
//	
//	
//
//}
