//package com.alibaba.middleware.mindw;
//
//import java.io.Serializable;
//import java.text.DecimalFormat;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.RaceUtils;
//import com.alibaba.middleware.race.jstorm.Global;
//import com.alibaba.middleware.race.model.OrderMessage;
//import com.alibaba.middleware.race.model.PaymentMessage;
//import com.taobao.tair.ResultCode;
//import com.taobao.tair.impl.DefaultTairManager;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class RaceBolt implements IRichBolt {
//
//	private static Logger LOG = Logger.getLogger(RaceBolt.class);
//
//	private DefaultTairManager tairManager;
//	
//	@Override
//	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
//		initTair();
//	}
//
//	/** initiate tair */
//	private void initTair() {
//
//		// 创建config server列表
//		List<String> confServers = new ArrayList<String>();
//		confServers.add(RaceConfig.TairConfigServer);
//		confServers.add(RaceConfig.TairSalveConfigServer); // 可选
//
//		// 创建客户端实例
//	    tairManager = new DefaultTairManager();
//		tairManager.setConfigServerList(confServers);
//
//		// 设置组名
//		tairManager.setGroupName(RaceConfig.TairGroup);
//
//		// 初始化客户端
//		tairManager.init();
//	}
//
//	/** 将结果写到Tari中 */
//	private void write(Serializable key, Serializable value) {
//		// 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间
//		int tryNum = 3;
//		for(int i=0;i<tryNum;++i){
//			ResultCode code = tairManager.put(RaceConfig.TairNamespace, key, value);
//			if(code==ResultCode.SUCCESS)
//				break;
//		}
//		LOG.info(key + " " + value);
//	}
//
//	@Override
//	public void execute(Tuple tuple) {
//
//		double money = tuple.getDouble(0);
//		long time = tuple.getLong(1);
//		short kind = tuple.getShort(2);
//
//		if (kind == Global.PAY_WIRE_KIND || kind == Global.PAY_PC_KIND)
//			dealPay(money, time, kind);
//		else if (kind == Global.TAOBAO_KIND)
//			dealTaobao(money, time, kind);
//		else if (kind == Global.TMALL_KIND)
//			dealTmall(money, time, kind);
//
//		// collector.ack(tuple);
//	}
//
//	/** 初始时间 */
//	private static final long INITIATE_TIME = -1;
//
//	// >>> 支付信息
//
//	/** 支付信息最新时间段 */
//	private long pay_last_minute = INITIATE_TIME;
//	/** 0 pc 支付总和 */
//	private double pcSum = 0;
//	/** 1 无线 支付总和 */
//	private double wireSum = 0;
//	/** double类型输出格式,保留两位小数 */
//	private DecimalFormat df = new DecimalFormat("#.00");
//
//	private void dealPay(double money, long time, short kind) {
//
//		// 结束时把最后一个值输出
//		if (time == Global.MESSAGE_END_TIME) {
//			// 每整分时刻无线和PC端总交易金额比值，官方说肯定有记录
//			double ratio = wireSum / pcSum;
//			ratio = Double.parseDouble(df.format(ratio));
//			write(RaceConfig.prex_ratio + pay_last_minute, ratio);
//			return;
//		}
//
//		// 转换为整分
//		Long minuteTime = (time / 60000) * 60;
//
//		// 是否要把之前的输出
//		if (minuteTime > pay_last_minute) {
//			if (pay_last_minute != INITIATE_TIME) { // 排除第一次接受数据
//				// 每整分时刻无线和PC端总交易金额比值，官方说肯定有记录
//				double ratio = wireSum / pcSum;
//				ratio = Double.parseDouble(df.format(ratio));
//				write(RaceConfig.prex_ratio + pay_last_minute, ratio);
//			}
//			// 更新 时间
//			pay_last_minute = minuteTime;
//		}
//
//		// 0 pc
//		if (kind == Global.PAY_PC_KIND)
//			pcSum += money;
//
//		// 1 无线
//		if (kind == Global.PAY_WIRE_KIND)
//			wireSum += money;
//
//	}
//
//	// >>> 淘宝订单数据
//
//	private long taobao_last_minute = INITIATE_TIME;
//	/** 每分钟的支付总额 */
//	private double taobao_minute_sum = 0;
//
//	private void dealTaobao(double money, long time, short kind) {
//
//		if (time == Global.MESSAGE_END_TIME) {
//			write(RaceConfig.prex_taobao + taobao_last_minute,taobao_minute_sum);
//			return;
//		}
//
//		// 转换为整分
//		Long minuteTime = (time / 60000) * 60;
//
//		// 是否要把之前的输出
//		if (minuteTime > taobao_last_minute) {
//			if (taobao_last_minute != INITIATE_TIME) { // 排除第一次接受数据
//				write(RaceConfig.prex_taobao + taobao_last_minute,
//						taobao_minute_sum);
//				taobao_minute_sum = 0;
//			}
//			// 更新 时间
//			taobao_last_minute = minuteTime;
//		}
//		taobao_minute_sum += money;
//
//	}
//
//	// >>> 天猫订单数据
//
//	private long tmall_last_minute = INITIATE_TIME;
//	/** 每分钟的支付总额 */
//	private double tmall_minute_sum = 0;
//
//	private void dealTmall(double money, long time, short kind) {
//
//		if (time == Global.MESSAGE_END_TIME) {
//			write(RaceConfig.prex_tmall + tmall_last_minute, tmall_minute_sum);
//			return;
//		}
//
//		// 转换为整分
//		Long minuteTime = (time / 60000) * 60;
//
//		// 是否要把之前的输出
//		if (minuteTime > tmall_last_minute) {
//			if (tmall_last_minute != INITIATE_TIME) { // 排除第一次接受数据
//				write(RaceConfig.prex_tmall + tmall_last_minute,tmall_minute_sum);
//				tmall_minute_sum = 0; // 清0
//			}
//			// 更新 时间
//			tmall_last_minute = minuteTime;
//		}
//		tmall_minute_sum += money;
//
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
//
//	@Override
//	public void cleanup() {}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		return null;
//	}
//	
//}
