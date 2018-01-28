package test.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BufferedFileRead implements FileRead {

	@Override
	public long read(String pathname) throws IOException {
		long tBegin = System.currentTimeMillis();
		
		BufferedReader bfr = new BufferedReader( new FileReader(pathname));
		String line = bfr.readLine();
		while (line != null) { // 一个一个字节读
			line = bfr.readLine();
		}
		if (bfr != null) {
			bfr.close();
		}
		
		long tEnd = System.currentTimeMillis();
		return tEnd - tBegin;
	}

}
