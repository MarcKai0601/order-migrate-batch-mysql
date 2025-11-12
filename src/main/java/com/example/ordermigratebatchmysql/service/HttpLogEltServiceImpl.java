package com.example.ordermigratebatchmysql.service;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.mapper.HttpLogEltMapper;
import com.example.ordermigratebatchmysql.mapper.WithdrawHttpLogEltMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Service
@RequiredArgsConstructor
public class HttpLogEltServiceImpl implements HttpLogEltService {

    private final HttpLogEltMapper orderMapper;
    private final WithdrawHttpLogEltMapper withdrawMapper;
    private final EltProperties props;
    private final PlatformTransactionManager txManager;

    @Override
    public int runOrderBatches() {
        return runBatches("order");
    }

    @Override
    public int runWithdrawBatches() {
        return runBatches("withdraw");
    }

    private int runBatches(String kind) {
        // 時間窗：上月1號 00:00 到 下月1號 00:00
        ZoneId zone = ZoneId.of(props.getZoneId());
        LocalDate today = LocalDate.now(zone);
        LocalDateTime start = today.withDayOfMonth(1).minusMonths(1).atStartOfDay();
        LocalDateTime end   = today.atStartOfDay();

        String runId = currentRunId();
        int processed = 0;

        // 只在第一批前「可觀測地」印一次缺口數
        int missing = ("order".equals(kind))
                ? orderMapper.countMissingForRange(start, end)
                : withdrawMapper.countMissingForRange(start, end);
        log.info("[ELT][{}][runId={}] pre-check missing={} | window=[{}, {})",
                kind, runId, missing, start, end);

        for (int i = 1; i <= props.getMaxBatchesPerRun(); i++) {
            long t0 = System.currentTimeMillis();
            final int batchId = i;

            int affected = executeWithRetry(kind, () -> {
                log.info("[ELT][{}][runId={}] batch#{} BEGIN | window=[{}, {}) | size={}",
                        kind, runId, batchId, start, end, props.getBatchSize());
                return doOneBatchTransactional(kind, start, end, props.getBatchSize());
            });

            long cost = System.currentTimeMillis() - t0;

            if (affected <= 0) {
                log.info("[ELT][{}][runId={}] no more rows | total={}", kind, runId, processed);
                break;
            }

            processed += affected;
            double qps = (affected * 1000.0) / Math.max(1, cost);

            log.info("[ELT][{}][runId={}] batch#{} END | affected={} | cost={} ms | ~{}/s | total={}",
                    kind, runId, batchId, affected, cost, String.format("%.0f", qps), processed);

            long slowMs = props.getSlowBatchMs() != null ? props.getSlowBatchMs() : 1500L;
            if (cost >= slowMs) {
                log.warn("[ELT][{}][runId={}] SLOW batch | batch#{} | cost={} ms (>= {} ms) | affected={}",
                        kind, runId, batchId, cost, slowMs, affected);
            }

            if (props.getPauseMs() > 0) {
                try { Thread.sleep(props.getPauseMs()); } catch (InterruptedException ignored) {}
            }
        }

        log.info("[ELT][{}][runId={}] SUMMARY | totalProcessed={}", kind, runId, processed);
        return processed;
    }

    private int doOneBatchTransactional(String kind, LocalDateTime start, LocalDateTime end, int batchSize) {
        TransactionTemplate tt = new TransactionTemplate(txManager);
        tt.setReadOnly(false);
        // 可選：設定隔離級別/逾時（遇到大量鎖競爭時可打開）
        // tt.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        // tt.setTimeout(30);

        return tt.execute(status -> {
            if ("order".equals(kind)) {
                // 推薦：INSERT IGNORE 版本（在 Mapper XML）
                return orderMapper.insertMissingForRange(start, end, batchSize);
            } else {
                return withdrawMapper.insertMissingForRange(start, end, batchSize);
            }
        });
    }

    @FunctionalInterface
    interface TxCallable { int call(); }

    private int executeWithRetry(String kind, TxCallable c) {
        String runId = currentRunId();
        int attempt = 0;
        while (true) {
            try {
                return c.call();
            } catch (RuntimeException e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                boolean retryable =
                        msg.contains("Deadlock found") || msg.contains("errno 1213") ||
                                msg.contains("Lock wait timeout exceeded") || msg.contains("errno 1205");

                if (retryable && attempt < props.getMaxRetry()) {
                    attempt++;
                    long backoff = props.getRetryBackoffBaseMs() * attempt;
                    log.warn("[ELT][{}][runId={}] RETRY {}/{} | backoff={} ms | err={}",
                            kind, runId, attempt, props.getMaxRetry(), backoff, firstLine(msg));
                    try { Thread.sleep(backoff); } catch (InterruptedException ignored) {}
                    continue;
                }
                log.error("[ELT][{}][runId={}] FAIL no-retry | err={}", kind, runId, msg, e);
                throw e;
            }
        }
    }

    private String firstLine(String s) {
        int p = s.indexOf('\n');
        return p >= 0 ? s.substring(0, p) : s;
    }

    // 從 ThreadLocal 暫存 runId（由 Runner 設定）；若沒有就印 "na"
    private static final ThreadLocal<String> RUN_ID = new ThreadLocal<>();
    public static void setRunId(String runId) { RUN_ID.set(runId); }
    public static void clearRunId() { RUN_ID.remove(); }
    private static String currentRunId() { return RUN_ID.get() == null ? "na" : RUN_ID.get(); }


//    public static void main(String[] args) {
//        ZoneId zone = ZoneId.of("Asia/Taipei");
//        LocalDate today = LocalDate.now(zone);
//        LocalDateTime start = today.withDayOfMonth(1).minusMonths(1).atStartOfDay();
//        LocalDateTime end   = today.atStartOfDay();
//
//        System.out.println(start);
//        System.out.println(end);
//
//    }
}