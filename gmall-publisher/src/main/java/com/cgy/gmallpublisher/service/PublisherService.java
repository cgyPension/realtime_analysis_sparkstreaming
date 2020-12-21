package com.cgy.gmallpublisher.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

public interface PublisherService {


    // 查询总数
    Long getDau(String date);

    // 获取每个小时的日活明细
    Map<String,Long> getHourDau(String date);

    BigDecimal getTotalAmount(String date);
    Map<String, BigDecimal> getHourTotalAmount(String date); // 想获得的展示效果不一样

    /**
     * 总数
     * 聚合结果
     * 详情
     * Map["tltal"->62,"detail"->List<Map>,"aggs"->Map["genderAgg"->...,"aggAgg"->...]]
     *
     * Map["tltal"->62,"detail"->List<Map>,"genderAgg"->...,"aggAgg"->...]
     */
   Map<String,Object> getDaleDetailAndAgg(
            String date,
            String keyword,
            int startpage,
            int size
    ) throws IOException;


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