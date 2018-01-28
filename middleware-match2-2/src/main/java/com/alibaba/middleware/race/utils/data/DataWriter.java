//package com.alibaba.middleware.race.file;
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
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//public class DataWriter {
//	
//	private final int DATA_REPO_WAIT_SIZE = 128 * 1024;
//	
//	private final int INFO_REPO_WAIT_SIZE = 1024 * 1024;
//	
//	private final ConcurrentLinkedQueue<Data> datas = new ConcurrentLinkedQueue<>();
//	
//	public void addAllData(List<Data> lsData){
//		while(datas.size()>DATA_REPO_WAIT_SIZE){
//			Thread.yield();
//		}
//		datas.addAll(lsData);
//		
//	}
//	
//	private final ConcurrentLinkedQueue<Info> infos = new ConcurrentLinkedQueue<>();
//	
//	public void addAllInfo(List<Info> lsInfo){
//		while(infos.size()>INFO_REPO_WAIT_SIZE){
//			Thread.yield();
//		}
//		infos.addAll(lsInfo);
//	}
//	
//	/**
//	 * 按文件名划分
//	 */
//	private Map<String, List<Data>> divideDataByFileName(
//			Collection<Data> lsData) {
//
//		Map<String, List<Data>> map = new HashMap<>();
//		for (Data data : lsData) {
//			if (map.containsKey(data.fileName)) {
//				map.get(data.fileName).add(data);
//			} else {
//				List<Data> datas = new ArrayList<>();
//				datas.add(data);
//				map.put(data.fileName, datas);
//			}
//		}
//		return map;
//	}
//
//	/**
//	 * 按文件名划分
//	 */
//	private Map<String, Map<String,List<String>>> divideInfoByFileName(
//			List<Info> lsInfo) {
//
//		// 文件MAP fileName -> key -> values
//		Map<String, Map<String,List<String>>>  fileMap = new HashMap<>();
//		
//		for (Info info : lsInfo) {
//			
//			if (!fileMap.containsKey(info.fileName)) {
//				fileMap.put(info.fileName, new HashMap<String,List<String>>());
//			}
//			
//			// 每行  key -> values
//			Map<String,List<String>> lineMap =  fileMap.get(info.fileName);
//			
//			if(!lineMap.containsKey(info.key)){
//				lineMap.put(info.key, new ArrayList<String>());
//			} 
//			
//			List<String> values = lineMap.get(info.key);
//			values.add(info.value);
//			
//		}
//		return fileMap;
//	}
//	
//	
//	private Map<String,Long> fileInfoMap = new HashMap<String,Long>();
//	
//
//	/**
//	 * 将map写入指定文件
//	 * 
//	 * @param filename
//	 *            文件名
//	 * @param lsData
//	 *            需要发送的数据
//	 */
//	public void writeDataToFile() {
//
//		List<Data> lsData = new ArrayList<Data>();
//		for(int i=0;i<DATA_REPO_WAIT_SIZE;++i){
//			Data data = datas.poll();
//			if(data==null)
//				break;
//			lsData.add(data);
//		}
//		
//		Map<String, List<Data>> map = divideDataByFileName(lsData);
//
//		for (String fileName : map.keySet()) {
//
//			try {
//
//				StringBuilder builder = new StringBuilder();
//				StringBuilder indexBuilder = new StringBuilder();
//				
//				for (Data data: map.get(fileName)) {
//					
//					builder.append(data.line);
//					
//					// 获得文件当前位置
//					long pos = 0;
//					if(fileInfoMap.containsKey(fileName)){
//						pos = fileInfoMap.get(fileName);
//					}
//					
//					int len = data.line.getBytes().length;
//					
//					indexBuilder.append(data.key);
//					indexBuilder.append(",");
//					indexBuilder.append(pos);
//					indexBuilder.append(",");
//					indexBuilder.append(len);
//					indexBuilder.append("\n");
//					
//					fileInfoMap.put(fileName, pos+len);
//				}
//
//				String content = builder.toString();
//				RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
//				FileChannel fcout = file.getChannel();
//				// fcout.write(ByteBuffer.wrap(content.getBytes()), fcout.size());
//				MappedByteBuffer mbb =fcout.map(MapMode.READ_WRITE, fcout.size(),content.getBytes().length);
//				mbb.put(content.getBytes());
//				fcout.close();
//				file.close();
//				
//				String indexContent = indexBuilder.toString();
//				RandomAccessFile indexFile = new RandomAccessFile(new File(fileName+"_"), "rw");
//				FileChannel indexFcout = indexFile.getChannel();
//				// indexFcout.write(ByteBuffer.wrap(indexContent.getBytes()), indexFcout.size());
//				MappedByteBuffer indexMbb = indexFcout.map(MapMode.READ_WRITE, indexFcout.size(),indexContent.getBytes().length);
//				indexMbb.put(indexContent.getBytes());
//				indexFcout.close();
//				indexFile.close();
//				
//
//			} catch (IOException e) {
//			}
//
//		}
//	}
//	
//	public static int maxInfoLineSize = 0;
//
//	/**
//	 * 在指定文件中添加一行
//	 * 
//	 * @param fileName
//	 *            需要添加记录的文件名
//	 * @param content
//	 *            内容
//	 * @return
//	 */
//	public boolean writInfoToFile() {
//		
//		List<Info> lsInfo = new ArrayList<Info>();
//		for(int i=0;i<INFO_REPO_WAIT_SIZE;++i){
//			
//			Info info = infos.poll();
//			if(info==null)
//				break;
//			lsInfo.add(info);
//		}
//		
//		Map<String, Map<String,List<String>>>  fileMap = divideInfoByFileName(lsInfo);
//		try {
//			
//			for (String fileName : fileMap.keySet()) {
//			
//				Map<String,List<String>> lineMap = fileMap.get(fileName);
//				
//				StringBuilder builder = new StringBuilder();
//				for (String key : lineMap.keySet()) {
//					builder.append(key);
//					List<String> values = lineMap.get(key);
//					int len =0;
//					for(String value : values){
//						builder.append(",");
//						builder.append(value);
//						len+=value.length()+1;
//					}
//					if(len>maxInfoLineSize)
//						maxInfoLineSize = len;
//					builder.append("\n");
//				}
//						
//				String content = builder.toString();
//				RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
//				FileChannel fcout = file.getChannel();
//				fcout.write(ByteBuffer.wrap(content.getBytes()), fcout.size());
//				fcout.close();
//				file.close();
//				
//			}
//			
//		} catch (IOException e) {}
//
//		return true;
//	}
//
//}
