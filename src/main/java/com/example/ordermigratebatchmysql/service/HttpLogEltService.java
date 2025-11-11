package com.example.ordermigratebatchmysql.service;

import com.example.ordermigratebatchmysql.mapper.HttpLogEltMapper;
import com.example.ordermigratebatchmysql.mapper.WithdrawHttpLogEltMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Service
public class HttpLogEltService {

    private final HttpLogEltMapper httpLogEltMapper;
    private final WithdrawHttpLogEltMapper withdrawHttpLogEltMapper;

    public HttpLogEltService(HttpLogEltMapper httpLogEltMapper, WithdrawHttpLogEltMapper withdrawHttpLogEltMapper) {
        this.httpLogEltMapper = httpLogEltMapper;
        this.withdrawHttpLogEltMapper = withdrawHttpLogEltMapper;
    }

    public void migratePrevAndThisMonthLoop(int batchSize, int maxBatchesPerRun, long pauseMs) {
        ZoneId zone = ZoneId.of("Asia/Taipei");
        LocalDate today = LocalDate.now(zone);
        LocalDateTime start = today.withDayOfMonth(1).minusMonths(1).atStartOfDay();
        LocalDateTime end   = today.withDayOfMonth(1).plusMonths(1).atStartOfDay();

        int ordertotal = 0;
        for (int i = 1; i <= maxBatchesPerRun; i++) {
            int n = migrateOneBatchbyOrderHttpLog(start, end, batchSize);
            if (n <= 0) {
                log.info("order沒有資料要搬移了（本次累計 {} 筆）。", ordertotal);
                return;
            }
            ordertotal += n;
            log.info("order 第 {} 批完成，affected={}，累計={}", i, n, ordertotal);
            if (pauseMs > 0) {
                try { Thread.sleep(pauseMs); } catch (InterruptedException ignored) {}
            }
        }
        log.warn("達到本次批次上限，order累計={}", ordertotal);

    }

    public void migratePrevAndThisMonthLoopbyWithdraw(int batchSize, int maxBatchesPerRun, long pauseMs) {
        ZoneId zone = ZoneId.of("Asia/Taipei");
        LocalDate today = LocalDate.now(zone);
        LocalDateTime start = today.withDayOfMonth(1).minusMonths(1).atStartOfDay();
        LocalDateTime end   = today.withDayOfMonth(1).plusMonths(1).atStartOfDay();

        int withdrawtotal = 0;
        for (int i = 1; i <= maxBatchesPerRun; i++) {
            int n = migrateOneBatchbyWithdrawOrderHttpLog(start, end, batchSize);
            if (n <= 0) {
                log.info("withdraw沒有資料要搬移了（本次累計 {} 筆）。", withdrawtotal);
                return;
            }
            withdrawtotal += n;
            log.info("withdraw 第 {} 批完成，affected={}，累計={}", i, n, withdrawtotal);
            if (pauseMs > 0) {
                try { Thread.sleep(pauseMs); } catch (InterruptedException ignored) {}
            }
        }
        log.warn("達到本次批次上限，withdraw累計={}", withdrawtotal);

    }


    @Transactional
    public int migrateOneBatchbyOrderHttpLog(LocalDateTime start, LocalDateTime end, int batchSize) {
        return httpLogEltMapper.insertMissingForRange(start, end, batchSize);
    }

    @Transactional
    public int migrateOneBatchbyWithdrawOrderHttpLog(LocalDateTime start, LocalDateTime end, int batchSize) {
        return withdrawHttpLogEltMapper.insertMissingForRange(start, end, batchSize);
    }

//    public static void main(String[] args) {
//        ZoneId zone = ZoneId.of("Asia/Taipei");
//        LocalDate today = LocalDate.now(zone);
//        System.out.println(today.withDayOfMonth(1).minusMonths(1).atStartOfDay());
//        System.out.println(today.withDayOfMonth(1).plusMonths(1).atStartOfDay());
//    }
}
