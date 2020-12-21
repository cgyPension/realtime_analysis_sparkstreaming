package com.cgy.gmall.common;

/**
 * @author GyuanYuan Cai
 * 2020/10/14
 * Description:
 */

/**
 * 订单业务数据最好单分区
 * 注意：order_info_topic默认还是输出到指定Kafka主题的一个kafka分区，因为多个分区并行可能会打乱binlog的顺序
 * 如果要提高并行度，首先设置kafka的分区数>1,然后设置canal.mq.partitionHash属性,设置副本属性
 *
 *
 * 行为日志可以用多分区
 * 注意：默认情况下，Kafka创建主题默认分区是1个，我这里修改为4个
 * bin/kafka-topics.sh --bootstrap-server hadoop202:9092 --create --topic gmall_start_bak --partitions 4 --replication-factor 3
 *
 * bin/kafka-topics.sh --bootstrap-server hadoop202:9092 --create --topic gmall_event_bak --partitions 4 --replication-factor 3
 */

public class Constant {
    public static final String STARTUP_TOPIC = "startup_topic";
    public static final String EVENT_TOPIC = "event_topic";
    public static final String ORDER_INFO_TOPIC = "order_info_topic";
    public static final String ORDER_DETAIL_TOPIC = "order_detail_topic";
}



