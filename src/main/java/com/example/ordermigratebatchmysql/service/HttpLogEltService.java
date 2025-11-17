package com.example.ordermigratebatchmysql.service;

import java.time.LocalDateTime;

public interface HttpLogEltService {

    /**
     * 搬移「支付訂單」的一個時間窗（例如某一天）
     * @return 此時間窗內實際搬移的筆數
     */
    int runOrderBatches(LocalDateTime start, LocalDateTime end, String runId);

    /**
     * 搬移「代付訂單」的一個時間窗（例如某一天）
     * @return 此時間窗內實際搬移的筆數
     */
    int runWithdrawBatches(LocalDateTime start, LocalDateTime end, String runId);

    /**
     * 預估指定時間窗內，還有多少「尚未搬移」的資料（只拿來觀察用）
     * kind = "order" / "withdraw"
     */
    int countMissing(String kind, LocalDateTime start, LocalDateTime end);
}