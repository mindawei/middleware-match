package com.alibaba.middleware.race;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.jstorm.Global;
import com.alibaba.middleware.race.jstorm.Message;
import com.alibaba.middleware.race.jstorm.MessageTuple;
import com.alibaba.middleware.race.jstorm.RaceBolt1;
import com.alibaba.middleware.race.jstorm.RaceBolt2;
import com.alibaba.middleware.race.jstorm.RaceSpout;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.jstorm.ValidInfo;
import com.alibaba.middleware.race.jstorm.ValidInfoTuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }

//    public static void main(String[] args){
//    	ValidInfoTuple infoTuple = new ValidInfoTuple();
//    	List<ValidInfo> lsInfos = new ArrayList<>();
//    	lsInfos.add(new ValidInfo(0, 0L, (short)1));
//    	lsInfos.add(new ValidInfo(1, 0L, (short)1));
//    	lsInfos.add(new ValidInfo(2, 0L, (short)1));
//    	lsInfos.add(new ValidInfo(3, 0L, (short)1));
//    	
//    	System.out.println(lsInfos.size());
//    	infoTuple.addValidInfos(lsInfos);
//    	System.out.println(infoTuple.getValidInfos().size());
//    	infoTuple.addValidInfos(new ArrayList<ValidInfo>());
//    	
//    	ValidInfoTuple infoTuple2 = readKryoObject(ValidInfoTuple.class,writeKryoObject(infoTuple));
//    	
//    	for(ValidInfo info : infoTuple2.getValidInfos()){
//    		System.out.println(info.getMoney());
//    	}
//    	
//    	
//  }
    
}
