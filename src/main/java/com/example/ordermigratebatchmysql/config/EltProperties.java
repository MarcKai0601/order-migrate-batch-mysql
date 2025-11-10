package com.example.ordermigratebatchmysql.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "elt")
public class EltProperties {
    private boolean enabled = true;
    private int batchSize = 150_000;
    private int maxBatchesPerRun = 50;
    private long pauseMs = 300;
    private String cron = "0 15 2 * * *";
    private String zoneId = "Asia/Taipei";
}
