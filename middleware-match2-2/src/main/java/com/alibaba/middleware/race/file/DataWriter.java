package com.alibaba.middleware.race.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataWriter {
	
	private final int INFO_REPO_WAIT_SIZE = 1024 * 1024;
	
	private final int INDEX_REPO_SIZE = 1024 * 1024;
		
	private final int MIN_OUTPUT_SIZE = 1024;
	
	private final ConcurrentLinkedQueue<Index> indexs = new ConcurrentLinkedQueue<>();
	
	public void addAllIndex(List<Index> lsIndex){
		while(indexs.size()>INDEX_REPO_SIZE){
			Thread.yield();
		}
		indexs.addAll(lsIndex);
	}
	
	public boolean isIndexsNeedOutput(){
		return indexs.size() >= MIN_OUTPUT_SIZE;
	}
	
	public void writeIndexToFile() {

		List<Index> lsIndex = new ArrayList<Index>();
		for (int i = 0; i < INDEX_REPO_SIZE; ++i) {
			Index index = indexs.poll();
			if (index == null)
				break;
			lsIndex.add(index);
		}
		

		Map<String, List<String>> map = new HashMap<>();
		for (Index index : lsIndex) {
			if (map.containsKey(index.indexFileName)) {
				map.get(index.indexFileName).add(index.content);
			} else {
				List<String> contents = new ArrayList<>();
				contents.add(index.content);
				map.put(index.indexFileName, contents);
			}
		}
		
		try {
			
			for (Map.Entry<String, List<String>> mapEntry : map.entrySet()) {
			
				String fileName = mapEntry.getKey();
				List<String> contents = mapEntry.getValue();
				
				StringBuilder builder = new StringBuilder();
				for (String content : contents) {
					builder.append(content);
				}
			
				String content = builder.toString();
				RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
				FileChannel fcout = file.getChannel();
				fcout.write(ByteBuffer.wrap(content.getBytes()), fcout.size());
				fcout.close();
				file.close();
			}
			
		} catch (IOException e) {}
		
	}
	
	
	private final ConcurrentLinkedQueue<Info> infos = new ConcurrentLinkedQueue<>();
	
	public boolean isInfoNeedOutput(){
		return infos.size() >= MIN_OUTPUT_SIZE;
	}
	
	public void addAllInfo(List<Info> lsInfo){
		while(infos.size()>INFO_REPO_WAIT_SIZE){
			Thread.yield();
		}
		infos.addAll(lsInfo);
	}
	

	/**
	 * 在指定文件中添加一行
	 * 
	 * @param fileName
	 *            需要添加记录的文件名
	 * @param content
	 *            内容
	 * @return
	 */
	public boolean writInfoToFile() {
		
		List<Info> lsInfo = new ArrayList<Info>();
		for(int i=0;i<INFO_REPO_WAIT_SIZE;++i){
			
			Info info = infos.poll();
			if(info==null)
				break;
			lsInfo.add(info);
		}
		
		
		// 文件MAP fileName -> key -> values
		Map<String, Map<String, List<String>>> fileMap = new HashMap<>();

		for (Info info : lsInfo) {

			if (!fileMap.containsKey(info.fileName)) {
				fileMap.put(info.fileName, new HashMap<String, List<String>>());
			}

			// 每行 key -> values
			Map<String, List<String>> lineMap = fileMap.get(info.fileName);

			if (!lineMap.containsKey(info.key)) {
				lineMap.put(info.key, new ArrayList<String>());
			}

			List<String> values = lineMap.get(info.key);
			values.add(info.value);

		}
		
		try {
			
			for (String fileName : fileMap.keySet()) {
			
				Map<String,List<String>> lineMap = fileMap.get(fileName);
				
				StringBuilder builder = new StringBuilder();
				for (String key : lineMap.keySet()) {
					builder.append(key);
					List<String> values = lineMap.get(key);
					for(String value : values){
						builder.append(",");
						builder.append(value);
					}
					builder.append("\n");
				}
						
				String content = builder.toString();
				RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
				FileChannel fcout = file.getChannel();
				fcout.write(ByteBuffer.wrap(content.getBytes()), fcout.size());
				fcout.close();
				file.close();
				
			}
			
		} catch (IOException e) {}

		return true;
	}


}
