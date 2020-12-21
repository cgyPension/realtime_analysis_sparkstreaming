package com.cgy.gmallpublisher.service;


import com.cgy.gmallpublisher.mapper.DauMapper;
import com.cgy.gmallpublisher.mapper.OrderMapper;
import com.cgy.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.transform.Result;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author GyuanYuan Cai
 * 2020/10/16
 * Description:
 */
@Service
public class PublisherServiceImp implements PublisherService{
    @Autowired
    DauMapper dau;
    @Override
    public Long getDau(String date) {
        return dau.getDau(date);

    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDau = dau.getHourDau(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map<String, Object> map:hourDau){
            String hour = (String) map.get("LOGHOUR");
            Long count = (Long) map.get("COUNT");
            result.put(hour,count);
        }
        return result;
    }

    @Autowired
    OrderMapper order;
    @Override
    public BigDecimal getTotalAmount(String date) {
        return order.getTotalAmount(date);
    }

    @Override
    public Map<String, BigDecimal> getHourTotalAmount(String date) {
        List<Map<String, Object>> hourAmount = order.getHourTotalAmount(date);
        Map<String, BigDecimal> result = new HashMap<>();
        for (Map<String,Object> map:hourAmount) {
            String hour = (String) map.get("CREATE_HOUR");
            BigDecimal amount = (BigDecimal) map.get("TOTAL_AMOUNT");
            result.put(hour,amount);
        }
        return result;
    }


    @Override
    public Map<String, Object> getDaleDetailAndAgg(String date, String keyword, int startpage, int size) throws IOException {
        HashMap<String, Object> result = new HashMap<>();
        // 1 获取es客户端
        JestClient client = ESUtil.getClient();
        // 2 通过客户端从es读取数据
        Search search = new Search.Builder(ESUtil.getQueryDSL(date, keyword, startpage, size))
                .addIndex("gmall_sale_detail")
                .addType("_doc")
                .build();
        SearchResult searchResult = client.execute(search);

        // 3 解析读到的数据
        // 3.1 解析出来总数
        Long total = searchResult.getTotal();
        result.put("total",total);
        // 3.2 把详情拿出来
        ArrayList<HashMap> details = new ArrayList<>();// 新建一个集合装详情
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            details.add(source);
        }
        result.put("details",details);
        // 3.3 拿出来聚合结果
        // 3.3.1 性别的聚合结果
        HashMap<String, Long> genderAgg = new HashMap<>(); // Map["M"->16, "F"->10]
        List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_gender")
                .getBuckets();

        for (TermsAggregation.Entry genderBucket : genderBuckets) {
            String gender = genderBucket.getKey();
            Long count = genderBucket.getCount();
            genderAgg.put(gender,count);
        }
        result.put("genderAgg",genderAgg);
        // 3.3.2 年龄的聚合结果
        HashMap<String,Long> ageAgg = new HashMap<>();// Map["M"->16, "F"->10]
        List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_age")
                .getBuckets();
        for (TermsAggregation.Entry ageBucket : ageBuckets) {
            String age = ageBucket.getKey();
            Long count = ageBucket.getCount();
            ageAgg.put(age,count);
        }
        result.put("ageAgg",ageAgg);
        // 4 关闭客户端
        client.shutdownClient();
        // 5 返回最终的Map集合

        // close 有时候会报空指针
        return result;
    }
}

/*
List<Map<String, Object>
    List(Map("loghour"->10, count->100), Map(....))
        +----------+--------+
    | LOGHOUR  | COUNT  |
    +----------+--------+
    | 14       | 182    |
    | 15       | 106    |
    | 17       | 11     |
    +----------+--------+

Map("10"->100, "11"->200, ...)

 */
