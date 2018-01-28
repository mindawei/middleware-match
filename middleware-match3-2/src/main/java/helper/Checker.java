package helper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.alibaba.middleware.race.sync.Constants;


public class Checker {
	
	//git@code.aliyun.com:mindawei/IncrementalSync-mdw.git
//	
//	http://middle2017.oss-cn-shanghai.aliyuncs.com/72634473t4/server.log.tar.gz
//		http://middle2017.oss-cn-shanghai.aliyuncs.com/72634473t4/client.log.tar.gz
	
	public static void main(String[] args) throws FileNotFoundException {
		
		
		String fileName1 = Constants.RESULT_HOME+"/参考 RESULT.txt";
		String fileName2 = Constants.RESULT_HOME+"/Result.rs";
	
		String md51 = MD5.getMd5ByFile(fileName1);
		String md52= MD5.getMd5ByFile(fileName2);
		
		if(!md51.equals(md52)){
			System.out.println("md5  不一致！");
			System.out.println("expect: "+md51);
			System.out.println("but: "+md52);
			System.exit(1);
		}
	
		String line1 = null;
		String line2 = null;
		int lineNum = 0;
		try (BufferedReader br1 = new BufferedReader(new FileReader(fileName1));
			BufferedReader br2 = new BufferedReader(new FileReader(fileName2));
				) {
			while ((line1 = br1.readLine()) != null) {
				lineNum++;
				line2 = br2.readLine();
				if(!line1.equals(line2)){
					System.out.println("Error at line "+lineNum);
					System.out.println("expect: "+line1);
					System.out.println("but: "+line2);
					System.exit(1);
				}
			}
			if(br2.readLine()!=null){
				System.out.println("Error: your answer is more!");
				System.exit(1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Your answer is OK!");
	}
}
