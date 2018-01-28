package test.io;

import java.io.IOException;

public interface FileRead {
	
	/**
	 * 读所有内容，返回用时
	 */
	long read(String pathname)throws IOException;
	
	
	
	
}
