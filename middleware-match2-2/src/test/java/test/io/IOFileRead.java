package test.io;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 普通文件读写
 */
public class IOFileRead implements FileRead{

	@Override
	public long read(String pathname) throws IOException {
		long tBegin = System.currentTimeMillis();
		
		FileInputStream is = new FileInputStream(pathname);
		int read = is.read();
		while (read != -1) { // 一个一个字节读
			read = is.read();
		}
		if (is != null) {
			is.close();
		}
		
		long tEnd = System.currentTimeMillis();
		return tEnd - tBegin;
	}

}
