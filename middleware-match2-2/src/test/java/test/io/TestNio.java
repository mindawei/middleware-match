package test.io;

import java.io.BufferedReader; 
import java.io.File; 
import java.io.FileInputStream; 
import java.io.IOException; 
import java.io.RandomAccessFile; 
import java.nio.ByteBuffer; 
import java.nio.channels.FileChannel; 
 
public class TestNio { 

 
    public static void readFileByLine(String FileName){ 
    	
		try {
			int _8K = 8 * 1024; // _8K
			String enterStr = "\n";

			File fin = new File(FileName);
			RandomAccessFile randomAccessFile = new RandomAccessFile(fin, "r");
			int bufSize = _8K;
			
			FileChannel fcin = randomAccessFile.getChannel();
			ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
			byte[] bs = new byte[bufSize];

			StringBuilder builder = new StringBuilder("");

			while (fcin.read(rBuffer) != -1) {
				
				// 将缓存的拷贝到数组中
				int rSize = rBuffer.position();
				rBuffer.rewind();
				rBuffer.get(bs);
				rBuffer.clear();
				
				String tempString = new String(bs, 0, rSize);
				int fromIndex = 0;
				int endIndex = 0;
				while ((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1) {
					// 当前行
					String line = tempString.substring(fromIndex, endIndex);
					// 之前残留的部分
					line = new String(builder.toString() + line);
					// line 为当前行
					
					
					// 删除之前的部分
					builder.delete(0, builder.length());
					fromIndex = endIndex + 1;
				}
				
				if (rSize > tempString.length()) { 
					// 最后没有读完
					builder.append(tempString.substring(fromIndex,tempString.length()));
				} else { 
					// 最后读完
					builder.append(tempString.substring(fromIndex, rSize));
				}
				
				
			}
			
			randomAccessFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    } 
 
    
}