package com.example.ordermigratebatchmysql;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(scanBasePackages = "com.example.ordermigratebatchmysql")
@MapperScan("com.example.ordermigratebatchmysql.mapper")
public class OrderMigrateBatchMysqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderMigrateBatchMysqlApplication.class, args);
    }

}
