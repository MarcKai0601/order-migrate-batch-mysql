package com.example.ordermigratebatchmysql.config;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Configuration
@EnableConfigurationProperties(EltProperties.class)
@MapperScan(basePackages = "com.example.ordermigratebatchmysql.mapper")
public class EltConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService eltExecutor(EltProperties props) {
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("elt-worker-" + t.getId());
            t.setDaemon(true);
            return t;
        };
        // 你在 props 裡的 workerThreads 會決定池大小
        return Executors.newFixedThreadPool(props.getWorkerThreads(), tf);
    }
}