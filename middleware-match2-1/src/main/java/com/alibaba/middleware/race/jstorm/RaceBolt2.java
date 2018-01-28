package com.alibaba.middleware.race.jstorm;

import java.text.DecimalFormat;
import java.util.*;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RaceBolt2 implements IRichBolt{
	
	private static final long serialVersionUID = 3L;

	// 输出相关
	private DefaultTairManager tairManager;
	// 发送缓存
	private Map<String, Double> sendedRepo = new HashMap<String, Double>();
	
	// 支付信息
	private Map<Long, Double> pcPayMap = new HashMap<Long, Double>();
	private Map<Long, Double> wirePayMap = new HashMap<Long, Double>();
	// 淘宝订单数据
	private Map<Long, Double> taobaoMap = new HashMap<Long, Double>();
	// 天猫订单数据
	private Map<Long, Double> tmallMap = new HashMap<Long, Double>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		initTair();

		new Timer().schedule(new TimerTask() {
			
			private DecimalFormat df = new DecimalFormat("#.00");
					
			@Override
			public void run() {
				
				synchronized (tairManager) { // map 临界区
					
					// pay 
					List<Long> timeKeys = new LinkedList<>();
					for (Long timeKey : pcPayMap.keySet()) {
						timeKeys.add(timeKey);
					}
					
					// 从小到大排序
					Collections.sort(timeKeys);
					
					double wireSum = 0;
					double pcSum = 0;
					
					for (Long timeKey : timeKeys) {

						wireSum += wirePayMap.containsKey(timeKey) ? wirePayMap.get(timeKey) : 0.0;
						pcSum += pcPayMap.containsKey(timeKey) ? pcPayMap.get(timeKey) : 0.0;

						double ratio = wireSum / pcSum;
						// 保留2位小数
						ratio = Double.parseDouble(df.format(ratio)); 
						
						write(RaceConfig.prex_ratio + timeKey, ratio);
						
					}
					
					// tmall
					for (Map.Entry<Long, Double> entry : tmallMap.entrySet())
						write(RaceConfig.prex_tmall + entry.getKey(), entry.getValue());
					
					// taobao
					for (Map.Entry<Long, Double> entry : taobaoMap.entrySet())
						write(RaceConfig.prex_taobao + entry.getKey(), entry.getValue());

				}
				
			}
			
		}, 30 * 1000, 30 * 1000); // 30s后开始，30s一次
		
	}

	
	/** 将结果写到Tari中 */
	private void write(String key, double value) {

		if (sendedRepo.containsKey(key) 
				&& sendedRepo.get(key).doubleValue() == value) { // 值更新过才输出
			return;
		} else {
			sendedRepo.put(key, value);
		}

		ResultCode code = tairManager.put(RaceConfig.TairNamespace, key, value);
		if (code != ResultCode.SUCCESS)
			sendedRepo.put(key, -1.0); // 未成功

	}

	@Override
	public void execute(Tuple tuple) {

		ValidInfoTuple validInfoTuple = (ValidInfoTuple) tuple.getValue(0);

		synchronized (tairManager) { // map 临界区

			// 处理每一条
			for (ValidInfo validInfo : validInfoTuple.getValidInfos()) {

				double money = validInfo.getMoney();
				long time = validInfo.getTime();
				short kind = validInfo.getKind();

				Map<Long, Double> map = null;

				if (kind == Global.PAY_WIRE_KIND) {
					map = wirePayMap;
				} else if (kind == Global.PAY_PC_KIND) {
					map = pcPayMap;
				} else if (kind == Global.TAOBAO_KIND) {
					map = taobaoMap;
				} else {
					map = tmallMap;
				}

				long minuteTime = (time / 60000) * 60; // 转换为整分

				if (map.containsKey(minuteTime)) {
					map.put(minuteTime, map.get(minuteTime) + money);
				} else {
					map.put(minuteTime, money);
				}

			}
			// End 处理每一条

		}// End  map 临界区
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void cleanup() {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/** initiate tair */
	private void initTair() {

		// 创建config server列表
		List<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer); // 可选

		// 创建客户端实例
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName(RaceConfig.TairGroup);

		// 初始化客户端
		tairManager.init();
	}

}
