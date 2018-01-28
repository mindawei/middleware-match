package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
	
	// log http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/42001sqkiw.tar.xz
	
	public static String teamcode = "42001sqkiw";

    public static String prex_tmall = "platformTmall_"+teamcode+"_";
    public static String prex_taobao = "platformTaobao_"+teamcode+"_";
    public static String prex_ratio = "ratio_"+teamcode+"_";


    public static String JstormTopologyName = "42001sqkiw";
    
    public static String MetaConsumerGroup = "42001sqkiw";
    
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 21925;
}
