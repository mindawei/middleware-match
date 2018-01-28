package com.alibaba.middleware.race.sync;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {
	
    public static void main(String[] args) throws Exception {
        // 等5s
        Thread.sleep(2000);
        QingClient qingClient = new QingClient(args[0],Constants.SERVER_PORT);
        try{
        	qingClient.action();
        }catch(Exception e){
        	e.printStackTrace();
        }
    }
}
