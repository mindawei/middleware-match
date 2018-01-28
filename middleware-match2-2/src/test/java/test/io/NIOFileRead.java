package test.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOFileRead implements FileRead {

	@Override
	public long read(String pathname) throws IOException {
		long tBegin = System.currentTimeMillis();
		
		FileInputStream is = new FileInputStream(pathname);
        FileChannel fi = is.getChannel();  
     
          
        ByteBuffer buffer = ByteBuffer.allocate(1024*1024);  
		while (true) {
			buffer.clear();
			int read = fi.read(buffer);
			if (read == -1) {
				break;
			}
			buffer.flip();
		}
		
		fi.close();
		is.close();
         

		long tEnd = System.currentTimeMillis();
		return tEnd - tBegin;
	}

}
