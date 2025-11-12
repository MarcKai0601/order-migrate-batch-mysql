package com.example.ordermigratebatchmysql.run;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.service.HttpLogEltService;
import com.example.ordermigratebatchmysql.service.HttpLogEltServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Component
@Profile("elt-run")
@RequiredArgsConstructor
public class HttpLogEltScheduler {

    private final HttpLogEltService service;  // 沿用你的 service 業務
    private final EltProperties props;        // 取 batchSize, maxBatches, pauseMs, workerThreads

    // 防重入（上一輪還沒跑完就到下個 cron）
    private final Object guard = new Object();
    private volatile boolean running = false;

    /**
     * 依 cron 週期觸發；cron/zone 直接沿用你的設定（有預設值）
     */
    @Scheduled(cron = "${elt.cron:0 */5 * * * *}", zone = "${elt.zoneId:Asia/Taipei}")
    public void trigger() {
        synchronized (guard) {
            if (running) {
                log.warn("[ELT][scheduler] previous run still running, skip this trigger.");
                return;
            }
            running = true;
        }

        String runId = UUID.randomUUID().toString().substring(0, 8);
        long t0 = System.currentTimeMillis();

        // 每次觸發都建立**自己的**執行緒池，不吃你 Runner 注入的那個（避免被關掉）
        int threads = Math.max(1, props.getWorkerThreads());
        ExecutorService pool = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r, "elt-sched-worker");
                    t.setDaemon(false);
                    return t;
                });

        log.info("=== [ELT][scheduler] START | batchSize={} maxBatches={} pauseMs={} threads={} | runId={} ===",
                props.getBatchSize(), props.getMaxBatchesPerRun(), props.getPauseMs(), threads, runId);

        Future<Integer> fOrder = pool.submit(wrapWithRunId(runId, "order", service::runOrderBatches));
        Future<Integer> fWithdraw = pool.submit(wrapWithRunId(runId, "withdraw", service::runWithdrawBatches));

        int total = 0;
        try { total += fOrder.get(); } catch (Exception e) { log.error("[ELT][order][runId={}] 任務失敗", runId, e); }
        try { total += fWithdraw.get(); } catch (Exception e) { log.error("[ELT][withdraw][runId={}] 任務失敗", runId, e); }

        long cost = System.currentTimeMillis() - t0;
        log.info("=== [ELT][scheduler] DONE | totalProcessed={} | cost={} ms | runId={} ===", total, cost, runId);

        // 良好收尾
        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) pool.shutdownNow();
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            running = false;
        }
    }

    /**
     * 與你 Runner 同風格：不靠 MDC，不用 logback pattern。
     * 直接呼叫 HttpLogEltServiceImpl 的 ThreadLocal runId。
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
