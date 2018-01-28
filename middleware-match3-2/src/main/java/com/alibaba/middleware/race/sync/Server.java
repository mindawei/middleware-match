package com.alibaba.middleware.race.sync;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {
	
	
	static FutureTask<long[]> readTask; 
	
//    public static int start = 100000;
//    public static int end   = 2000000;
    
    public static void main(String[] args) throws InterruptedException {
//        start = Integer.parseInt(args[2]);
//        end = Integer.parseInt(args[3]);
        
        // 异步读任务
        Callable<long[]> reader = new Callable<long[]>() {
			@Override
			public long[] call() throws Exception {
				return Reader.readMessage();
			}
		};
		readTask = new FutureTask<>(reader);
	   
	 
	    try {
	    	new Thread(readTask).start(); 
	    	QingServer qingServer = new QingServer(Constants.SERVER_PORT);
		    qingServer.service();
	    } catch (Exception e) {
			e.printStackTrace();
		}
        
    }
}
