package com.example.ordermigratebatchmysql.run;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.service.HttpLogEltService;
import com.example.ordermigratebatchmysql.service.HttpLogEltServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@Profile("elt-run")
@RequiredArgsConstructor
public class HttpLogEltRunner implements CommandLineRunner {

    private final HttpLogEltService service;
    private final EltProperties props;

    @Override
    public void run(String... args) {
        String runId = UUID.randomUUID().toString().substring(0, 8);

        ZoneId zone = ZoneId.of(props.getZoneId());
        LocalDate today = LocalDate.now(zone);

        // 這裡先做一個「上個月」的範例：上月1號 00:00 到 本月1號 00:00
        LocalDateTime start = today.withDayOfMonth(1).minusMonths(1).atStartOfDay();
        LocalDateTime end   = today.withDayOfMonth(1).atStartOfDay();

        log.info("=== [ELT] START | window=[{}, {}) | batchSize={} maxBatches={} | runId={} ===",
                start, end, props.getBatchSize(), props.getMaxBatchesPerRun(), runId);

        // 1) 啟動前先抓「剩餘筆數」（order / withdraw 各自算）
        int orderMissing    = service.countOrderMissing(start, end);
        int withdrawMissing = service.countWithdrawMissing(start, end);

        log.info("[ELT][runId={}] PRECHECK | orderMissing={} | withdrawMissing={}",
                runId, orderMissing, withdrawMissing);

        int orderMoved    = runKind("order", runId, start, end, orderMissing);
        int withdrawMoved = runKind("withdraw", runId, start, end, withdrawMissing);

        log.info("=== [ELT] DONE | runId={} | orderMoved={} / {} | withdrawMoved={} / {} ===",
                runId, orderMoved, orderMissing, withdrawMoved, withdrawMissing);
    }

    private int runKind(String kind,
                        String runId,
                        LocalDateTime start,
                        LocalDateTime end,
                        int totalMissingAtStart) {

        int totalMoved = 0;

        for (int batch = 1; batch <= props.getMaxBatchesPerRun(); batch++) {
            long t0 = System.currentTimeMillis();

            int affected;
            if ("order".equals(kind)) {
                affected = service.runOneOrderBatch(start, end);
            } else {
                affected = service.runOneWithdrawBatch(start, end);
            }

            long cost = System.currentTimeMillis() - t0;

            if (affected <= 0) {
                log.info("[ELT][{}][runId={}] no more rows | totalMoved={} | batch#={}",
                        kind, runId, totalMoved, batch);
                break;
            }

            totalMoved += affected;
            double qps = (affected * 1000.0) / Math.max(cost, 1);

            log.info("[ELT][{}][runId={}] batch#{} END | affected={} | cost={} ms | ~{}/s | totalMoved={} / {}",
                    kind, runId, batch, affected, cost, String.format("%.0f", qps), totalMoved, totalMissingAtStart);

            long pauseMs = props.getPauseMs();
            if (pauseMs > 0) {
                try {
                    Thread.sleep(pauseMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[ELT][{}][runId={}] interrupted between batches", kind, runId);
                    break;
                }
            }
        }

        return totalMoved;
    }
}