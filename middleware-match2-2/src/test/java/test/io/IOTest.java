package test.io;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * IO测试
 */
public class IOTest {
	public static void main(String[] args) throws IOException, InterruptedException {
//		String pathname = "./order_records.txt";
//		
//		Set<String> values = new HashSet<String>();
		
//		long bit = 10; 
//		long val = 900;
//		System.out.println(val>>bit);
//		System.out.println(val - (val>>bit<<bit));
		
//		String in = "ap-9522-6b6051b5d814,1471049969,592217865,1477237404,624456746,1471058071,592233714,1477244488,624470629,1470693352,591511730,1475012639,610060942,1478073648,626119142,1475552729,611127938,1478733190,627441527,1478735139,627445562,1478881133,627738491,1478881586,627739237,1475048965,610129141,1478745933,627465878,1475579016,611180355,1476835593,623660258,1476261356,612527287,1476840549,623669482,1472116594,604330166";
//		System.out.println(in.length());
		
		String test = "1231as";
		System.out.println(test.length());
		System.out.println(test.getBytes().length);
		
	}
}
