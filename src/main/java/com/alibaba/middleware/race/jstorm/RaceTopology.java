package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;

import backtype.storm.Config;

import org.slf4j.LoggerFactory;

import backtype.storm.StormSubmitter;

import com.alibaba.middleware.race.RaceConfig;
import backtype.storm.topology.TopologyBuilder;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {
	
    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
    	
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceSpout(), 1);
        builder.setBolt("bolt1", new RaceBolt1(), 1).noneGrouping("spout");
        builder.setBolt("bolt2", new RaceBolt2(), 1).noneGrouping("bolt1");
       
        Config conf = new Config();
        int max_worker_num = 4;
        conf.put(Config.TOPOLOGY_WORKERS, max_worker_num);
        
        // 设置表示acker的并发数,acker数至少大于0
//        int ackerParal = 1;
//        Config.setNumAckers(conf, ackerParal);
        
        // 设置topolog模式为分布式，这样topology就可以放到JStorm集群上运行
        // conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builder.createTopology());
            //LOG.info("StormSubmitter.submitTopology() done!");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.info(e.toString());
            throw e;
        }
        
    }
}