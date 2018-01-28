package com.alibaba.middleware.race.utils.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *@Description 文件输入输出工具类
 *@Author mindw
 *@Since 2016年7月11日
 *@Version 0.0.1
 */
public class IOUtil {

	/**
	 * 返回文件目录可用的字节数，如果是D:\\text\\，返回的是D:\\的可用空间 <br>
	 * 文件不存在返回0
	 */
	public static long getFolderFreeSpaceInByte(String storeFolder) {
		File file = new File(storeFolder);
		if(file.exists()){
			return file.getFreeSpace();
		}else{
			return 0;
		}
	}
	
	/**
	 * 返回文件目录可用的大小（GB），如果是D:\\text\\，返回的是D:\\的可用空间 <br>
	 * 文件不存在返回0
	 */
	public static long getFolderFreeSpaceInGB(String storeFolder) {
		return getFolderFreeSpaceInByte(storeFolder) / 1024 / 1024 / 1024;
	}
	
	
	/** 
	 * 获得写文件的对象，如果文件存在，则覆盖输出
	 */
	public static PrintWriter getPrintWriter(String outFilePath) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(outFilePath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println(outFilePath+" not found!");
			System.exit(0);
		}
		return out;
	}
	
	/** 添加文件 */
	public static FileWriter getAppednFileWriter(String outFilePath) {
		FileWriter fileWriter = null;
		try {
			//如果文件存在，则追加内容；如果文件不存在，则创建文件
			File f=new File(outFilePath);
			fileWriter = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileWriter;
	}
	
}
