package com.cgy.gmallpublisher.contronal;

import com.alibaba.fastjson.JSON;
import com.cgy.gmallpublisher.bean.Option;
import com.cgy.gmallpublisher.bean.SaleInfo;
import com.cgy.gmallpublisher.bean.Stat;
import com.cgy.gmallpublisher.service.PublisherService;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author GyuanYuan Cai
 * 2020/10/16
 * Description:
 */

@RestController
public class PublisherController {

    @Autowired
    PublisherService service;  // 反射 创建实现类的对象

    @GetMapping("/realtime-total")  //前端接口的请求路径
    public String realtimeTotal(String date){
        Long total = service.getDau(date);

        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();

        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",total);
        result.add(map1);

        Map<String,Object> map2 = new HashMap<>();
        map2.put("id","new_mid");
        map2.put("name","新增设备");
        map2.put("value",service.getTotalAmount(date));
        result.add(map2);

        Map<String, Object> map3 = new HashMap<>();  //  当天累计
        map3.put("id","order_amount");
        map3.put("name","新增交易额");
        map3.put("value",service.getTotalAmount(date));
        result.add(map3);

        return JSON.toJSONString(result); // 返回json格式给前端
    }

    @GetMapping("/realtime-hour")
    public String realtimeHour(String id,String date){
        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today",today);
            result.put("yesterday",yesterday);
            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)) {  //  每小时
            Map<String, BigDecimal> today = service.getHourTotalAmount(date);
            Map<String, BigDecimal> yesterday = service.getHourTotalAmount(getYesterday(date));

            HashMap<String, Map<String, BigDecimal>> result = new HashMap<>();
            result.put("today",today);
            result.put("yesterday",yesterday);
            return JSON.toJSONString(result);
        }
        return "";
    }

    @GetMapping("/sale_detail")
    public String saleDetail(String date, String keyword, int startpage, int size) throws IOException {
        Map<String, Object> saleDetailAndAgg = service.getDaleDetailAndAgg(date, keyword, startpage, size);
        Long total = (Long) saleDetailAndAgg.get("total");
        List<HashMap> detail = (List<HashMap>) saleDetailAndAgg.get("details");
        Map<String, Long> ageAgg = (Map<String, Long>) saleDetailAndAgg.get("ageAgg");
        Map<String, Long> genderAgg = (Map<String, Long>) saleDetailAndAgg.get("genderAgg");

        SaleInfo saleInfo = new SaleInfo();

        // 1 设置总数
        saleInfo.setTotal(total);
        // 2 设置详情
        saleInfo.setDetail(detail);
        // 3 饼图
        // 3.1 设置性别饼图
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        for (Map.Entry<String, Long> entry : genderAgg.entrySet()) {
            String gender = entry.getKey().equals("M") ? "男" : "女";
            Long count = entry.getValue();
            Option option = new Option(gender, count);
            genderStat.addOption(option);
        }
        saleInfo.addStat(genderStat);
        // 3.2 设置年龄饼图
        Stat ageStat = new Stat();
        ageStat.setTitle("用户年龄占比");
        //  es 只能把每个年龄的购买统计出来, 但是无法统计出来 20-39 这个10 个年龄的数据
        //  我们可以在代码中来统计这个 10 个年龄的数据
        ageStat.addOption(new Option("20岁以下",0L));
        ageStat.addOption(new Option("20岁到30岁",0L));
        ageStat.addOption(new Option("30岁以上",0L));
        for (String age : ageAgg.keySet()) {
            // 在es里是每个年龄段都统计
            int a = Integer.parseInt(age);
            Long count = ageAgg.get(age);
            if (a<20) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(opt.getValue()+count);
            } else if (a < 30) {//
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(opt.getValue() + count);
            } else {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(opt.getValue() + count);
            }
        }
        saleInfo.addStat(ageStat);
        return JSON.toJSONString(saleInfo);
    }

    /**
     * 计算昨天的年月日
     * @param date
     * @return
     */
    private String getYesterday(String date) {
        return LocalDate.parse(date).plusDays(-1).toString();
    }


}




