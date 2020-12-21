package com.cgy.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;


@SpringBootApplication
// 增加扫描包
@MapperScan(basePackages = "com.cgy.gmallpublisher.mapper")
public class GmallPublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallPublisherApplication.class, args);
	}

}
