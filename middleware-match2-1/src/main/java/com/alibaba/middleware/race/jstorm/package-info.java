/**
 * 一些说明
 */
package com.alibaba.middleware.race.jstorm;

/**
 >> worker
 backtype.storm.Config.setNumWorkers(int workers)是设置worker数目，
   表示这个Topology运行在多个个jvm（一个jvm是一个进程，即一个worker）；
 
 >> Task
 TopologyBuilder.setSpout(String id, IRichSpout spout,Number parallelism_hint) 和
 setBolt(String id, IRichBolt bolt,Number parallelism_hint)中的参数 parallelism_hint
   表示这个spout或bolt有多少个实例，即对应多少个线程执行，一个实例对应一个线程。
 
 >> 资源
     在JStorm中，资源类型分为4种， CPU, Memory，Disk， Port， 不再局限于Storm的port。
     即一个supervisor可以提供多少个CPU slot，多少个Memory slot， 多少个Disk slot， 多少个Port slot
     
     一个worker就消耗一个Port slot， 默认一个task会消耗一个CPU slot和一个Memory slot
    当task执行任务较重时，可以申请更多的CPU slot，
    当task需要更多内存时，可以申请更多的内存slot，
    当task磁盘读写较多时，可以申请磁盘slot，则该磁盘slot给该task独享
 
 */

/** >> 分组说明

	fieldsGrouping 
	globalGrouping – target component第一个task
	shuffleGrouping – 自定义random，更平均
	localOrShuffleGrouping
	noneGrouping  -- 调用random
	allGrouping   --  发送给target component所有task
	directGrouping – 指定目标task
	customGrouping – 接口CustomStreamGrouping

*/