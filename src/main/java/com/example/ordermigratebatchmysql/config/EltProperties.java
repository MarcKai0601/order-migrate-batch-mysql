package com.example.ordermigratebatchmysql.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "elt")
public class EltProperties {
    private boolean enabled = true;
    private int batchSize = 10000;
    private int maxBatchesPerRun = 1000;
    private long pauseMs = 200;
    private String cron = "0 */5 * * * *";
    private String zoneId = "Asia/Taipei";
}
