// src/main/java/com/example/ordermigratebatchmysql/config/EltProperties.java
package com.example.ordermigratebatchmysql.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "elt")
public class EltProperties {
    private boolean enabled = true;

    /** 每批筆數：先用 1000~3000，若鎖競爭多，先降到 500~1000 試 */
    private int batchSize = 5000;

    /** 單次 run 的批次上限（避免長時間持續佔用資源） */
    private int maxBatchesPerRun = 200;

    /** 批次間隙（毫秒）：輕微喘息，降低長交易占用；可從 100~300 起 */
    private long pauseMs = 300;

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

    /**
     * 🚦當「單日」預估缺口超過這個值時，會自動從「整天」切成「半天半天」搬。
     * 例：200_000 = 20 萬筆以上就用半天切。
     */
    private int halfDaySwitchThreshold = 100_000;

    /**
     * 🚦當「半天窗口」預估缺口再超過這個值時，會把該半天切成「一小時一小時」搬。
     * 例：80_000 = 8 萬筆以上就用每小時切。
     */
    private int hourSwitchThreshold = 50_000;

//    /** 一天缺口超過這個值，就不要用「整天」一次搬，改成先切成半天 */
//    private int halfDaySwitchThreshold = 30000;
//
//    /** 半天缺口超過這個值，就不要用「半天」，改成再切成每小時 */
//    private int hourSwitchThreshold = 20000;

}
