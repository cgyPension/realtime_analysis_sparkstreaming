package com.cgy.gmalllogger.controller;

import com.cgy.gmall.common.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;





/**
 * @author GyuanYuan Cai
 * 2020/10/14
 * Description:
 */
//    @RequestMapping(value = "/log", method = RequestMethod.POST)
//    @ResponseBody  //表示返回值是一个 字符串, 而不是 页面名

@RestController // 等价于: @RequestMapping(value = "/log", method = RequestMethod.POST)
public class LoggerController {
    @PostMapping("/log")
    public String doLog(String log) {
        // 1. 给数据添加时间戳
        log = addTS(log);
        // 2. 把数据落盘 (将来离线处理可以使用)
        saveToDisk(log);
        // 3. 直接写入到kafka  1.得到生产者 2. 通过生产者向kafka写数据
        sendToKafka(log);
        return "success";
    }

    // 自动注入
    @Autowired
    KafkaTemplate kafka;  // springboot 提供的kafka连接方法

    /**
     * 把日志发送到kafka
*     *
     * @param log
     */
    private void sendToKafka(String log) {
        // 不同的日志, 写到不同的topic中
        /*String topic = Constant.EVENT_TOPIC;
        if (log.contains("startup")) {
            topic = Constant.STARTUP_TOPIC;
        }*/
        String topic = log.contains("startup") ? Constant.STARTUP_TOPIC : Constant.EVENT_TOPIC;
        kafka.send(topic, log);
    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /**
     * 把日志写入到磁盘
     *
     * @param log
     */
    private void saveToDisk(String log) {
        logger.error(log);
        logger.warn(log);
    }

    /**
     * 给日志添加时间戳
     *
     * @param log
     * @return
     */
    private String addTS(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }
}

/*
logging 换成 log4j
 */