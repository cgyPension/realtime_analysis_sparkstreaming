package com.cgy.gmallpublisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderMapper {
    BigDecimal getTotalAmount(String date);

    // MyBasit 会把sql查询的结果集自动映射成List集合
    // 接着去资源文件夹的映射文件里写sql，映射文件相当于这个文件的实现类
    // 大体的业务逻辑也是在映射层做，其他基本上是格式类型转换
    // 服务层也要同时写接口
    // 服务实现层 自动注入 反射调用映射文件生成对象
    List<Map<String, Object>> getHourTotalAmount(String date);
}
