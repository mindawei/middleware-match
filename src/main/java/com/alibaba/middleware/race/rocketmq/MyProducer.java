package com.alibaba.middleware.race.rocketmq;


public class MyProducer {
/*
淘宝订单消息                                                                             
        订单id     买家ID    商品ID        卖家ID        订单时间                总价            对应的整分时间戳
        taobaoId1   buy1      productId1    salerId1      1467082096000       100.0           2016/6/28 10:48
        taobaoId2   buy2      productId2    salerId2      1467082156123       10.0          2016/6/28 10:49
        taobaoId3   buy3      productId3    salerId3      1467082156400        200.0          2016/6/28 10:49

        天猫订单消息
        订单id     买家ID    商品ID         卖家ID         订单时间                总价
        tmallId1   buy6      productId11    salerId6      1467082096000        110.0            2016/6/28 10:48
        tmallId2   buy1      productId21    salerId2      1467082156123        100.0         2016/6/28 10:49
        tmallId3   buy8      productId31    salerId2      1467082156400        500.0           2016/6/28 10:49

        付款消息
        订单ID       金额        来源       支付平台     创建时间              对应的整分时间戳
        taobaoId1   50.0         支付宝       pc         1467082096001    2016/6/28 10:48
        tmallId1    110.0         银联        wire       1467082096001    2016/6/28 10:48
        taobaoId1   50.0         银联         wire       1467082097001    2016/6/28 10:48
        taobaoId2   5.0         其他         pc         1467082156300    2016/6/28 10:49
        taobaoId2   5.0         其他         pc         1467082157300    2016/6/28 10:49
        tmallId2    100.0         支付宝       pc        1467082158000    2016/6/28 10:49
        tmallId3    500.0         支付宝       wire      1467082156400    2016/6/28 10:49
        taobaoId3   200.0        支付宝       pc         1467082156400     2016/6/28 10:49

        写入tair的结果是
        platformTaobao_teamcode_1467082080    100
        platformTaobao_teamcode_1467082140    210
        platformTmall_teamcode_1467082080    110
        platformTmall_teamcode_1467082140    600
        ratio_teamcode_1467082080    3.2
        ratio_teamcode_1467082140    1.83

 */
	
	
	
}
