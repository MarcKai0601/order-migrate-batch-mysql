// src/main/java/com/example/ordermigratebatchmysql/config/EltProperties.java
package com.example.ordermigratebatchmysql.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "elt")
public class EltProperties {
    private boolean enabled = true;

    /** 每批筆數：先用 1000~3000，若鎖競爭多，先降到 500~1000 試 */
    private int batchSize = 10000;

    /** 單次 run 的批次上限（避免長時間持續佔用資源） */
    private int maxBatchesPerRun = 200;

    /** 批次間隙（毫秒）：輕微喘息，降低長交易占用；可從 100~300 起 */
    private long pauseMs = 150;

    /** 兩條工作線（order / withdraw）並行即可；若再分片才需要 >2 */
    private int workerThreads = 2;

    /** 死鎖/鎖等待重試上限 */
    private int maxRetry = 5;

    /** 重試退避基準(ms)；實際 backoff = base * attempt（線性，可自行換指數） */
    private long retryBackoffBaseMs = 200;

    /** 是否嘗試 SELECT ... FOR UPDATE SKIP LOCKED（MySQL 8.0+ 才有；此情境多為 INSERT…SELECT，不一定適用） */
    private boolean useSkipLocked = false;

    /** 時區字串 */
    private String zoneId = "Asia/Taipei";

    /**慢批阈值 */
    private Long slowBatchMs = 1500L;

}
