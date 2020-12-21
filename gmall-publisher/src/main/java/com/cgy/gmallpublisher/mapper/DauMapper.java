package com.cgy.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/*
从数据库查询数据的接口
 */
public interface DauMapper {
    // 查询日活总数
    long getDau(String date);
    // 查询小时明细
    List<Map<String,Object>> getHourDau(String date);

}
/*
+----------+--------+
| LOGHOUR  | COUNT  |
+----------+--------+
| 14       | 182    |
| 15       | 106    |
| 17       | 11     |
+----------+--------+



 */