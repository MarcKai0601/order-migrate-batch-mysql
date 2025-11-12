package com.example.ordermigratebatchmysql.service;

public interface HttpLogEltService {
    int runOrderBatches();     // 回傳本輪處理總筆數
    int runWithdrawBatches();  // 回傳本輪處理總筆數
}
