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
    private final ExecutorService eltExecutor; // 由 EltConfig 注入

    @Override
    public void run(String... args) {
        String runId = java.util.UUID.randomUUID().toString().substring(0, 8);
        long t0 = System.currentTimeMillis();

        log.info("=== [ELT] START | batchSize={} maxBatches={} pauseMs={} threads={} | runId={} ===",
                props.getBatchSize(), props.getMaxBatchesPerRun(), props.getPauseMs(), props.getWorkerThreads(), runId);

        // 提交兩條 worker
        Future<Integer> fOrder = eltExecutor.submit(wrapWithRunId(runId, "order", service::runOrderBatches));
        Future<Integer> fWithdraw = eltExecutor.submit(wrapWithRunId(runId, "withdraw", service::runWithdrawBatches));

        int total = 0;
        try { total += fOrder.get(); } catch (Exception e) { log.error("[ELT][order][runId={}] 任務失敗", runId, e); }
        try { total += fWithdraw.get(); } catch (Exception e) { log.error("[ELT][withdraw][runId={}] 任務失敗", runId, e); }

        long cost = System.currentTimeMillis() - t0;
        log.info("=== [ELT] DONE | totalProcessed={} | cost={} ms | runId={} ===", total, cost, runId);

        // 若這個池是專屬 run 生命週期才關；若單例共享多次 run，這邊不要關
        eltExecutor.shutdown();
        try {
            if (!eltExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                eltExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            eltExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 把 Callable 包起來：設定/清除 runId，並在開始/結束印簡要 log。
     * 不用 MDC、不用 logback pattern；所有 runId/kind 都寫死在訊息字串裡。
     */
    private <T> Callable<T> wrapWithRunId(String runId, String kind, Callable<T> task) {
        return () -> {
            HttpLogEltServiceImpl.setRunId(runId);
            log.info("[ELT][{}][runId={}] worker start", kind, runId);
            try {
                return task.call();
            } finally {
                log.info("[ELT][{}][runId={}] worker done", kind, runId);
                HttpLogEltServiceImpl.clearRunId();
            }
        };
    }
}