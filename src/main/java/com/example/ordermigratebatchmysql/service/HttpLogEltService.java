package com.example.ordermigratebatchmysql.service;

import java.time.LocalDateTime;

public interface HttpLogEltService {

    int countOrderMissing(LocalDateTime start, LocalDateTime end);

    int countWithdrawMissing(LocalDateTime start, LocalDateTime end);

    /** 執行一批 order 搬移，回傳本批實際搬了幾筆 */
    int runOneOrderBatch(LocalDateTime start, LocalDateTime end);

    /** 執行一批 withdraw 搬移，回傳本批實際搬了幾筆 */
    int runOneWithdrawBatch(LocalDateTime start, LocalDateTime end);
}