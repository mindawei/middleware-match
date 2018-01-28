package preliminary.demo;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.Global;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.tuple.Tuple;


public class Test {
	
	private DefaultTairManager obj = new DefaultTairManager();

	List<Integer> ls = new ArrayList<>();
	
	public void run(){

		new Timer().schedule(new TimerTask() {
			int index =1;
			@Override
			public void run() {
				
				synchronized (obj) { // map 临界区
					ls.add(index++);
					if(index>2&&index%2==0)
					ls.remove(1);
				}
				
				
				
			}
			
		}, 1000, 10); // 30s后开始，30s一次
		
		
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				
				synchronized (obj) { // map 临界区
					for(int i: ls)
				
					System.out.print(i+" ");
//						System.out.println();
				}
				
			
				
			}
			
		}, 999, 10); // 30s后开始，30s一次
	
//	}
	}
	public static void main(String[] args) {
//		new Test().run();
//		while(true){
//		
//		}
		Map<Integer,Integer> map = new HashMap<Integer, Integer>();
		long t1= System.currentTimeMillis();
		for(int i=0;i<1200*10000;++i){
			map.put(i, i);
			if(map.containsKey(i)){
				
			}
//			if(i%2==0)
//			map.remove(i);
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println(t2-t1);
		
		// 2886
		// 19496

		
		
	}
	
	/***
	 * @param time "2015/05/26 19:50:00"
	 * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * @description warn:　will exist the program when exception occurs
	 */
    public static long timeToMilliseconds1(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");  
        long startMillisecond = -1;
		try {
			startMillisecond = sdf.parse(time).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(0);
		}
        return startMillisecond;
    }
	
}
