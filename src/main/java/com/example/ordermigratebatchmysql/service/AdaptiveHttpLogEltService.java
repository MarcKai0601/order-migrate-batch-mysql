package com.example.ordermigratebatchmysql.service;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.mapper.HttpLogEltMapper;
import com.example.ordermigratebatchmysql.mapper.WithdrawHttpLogEltMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class AdaptiveHttpLogEltService {

    private final HttpLogEltMapper orderMapper;
    private final WithdrawHttpLogEltMapper withdrawMapper;
    private final EltProperties props;
    private final PlatformTransactionManager txManager;

    // ==== 給 Runner 用的 API ====

    public int countMissing(String kind, LocalDateTime start, LocalDateTime end) {
        if ("order".equals(kind)) {
            return orderMapper.countMissingForRange(start, end);
        } else if ("withdraw".equals(kind)) {
            return withdrawMapper.countMissingForRange(start, end);
        } else {
            throw new IllegalArgumentException("unknown kind: " + kind);
        }
    }

    public int runOrderBatches(LocalDateTime start, LocalDateTime end, String runId) {
        return runBatchesAdaptive(
                "order",
                start,
                end,
                runId,
                batchSize -> orderMapper.insertMissingForRange(start, end, batchSize)
        );
    }

    public int runWithdrawBatches(LocalDateTime start, LocalDateTime end, String runId) {
        return runBatchesAdaptive(
                "withdraw",
                start,
                end,
                runId,
                batchSize -> withdrawMapper.insertMissingForRange(start, end, batchSize)
        );
    }

    // ==== 共用：自動調整 batchSize 的主程式 ====

    private int runBatchesAdaptive(
            String kind,
            LocalDateTime start,
            LocalDateTime end,
            String runId,
            BatchExecutor executor
    ) {
        int totalMoved = 0;

        // 動態 batch size，起點先用設定值
        int dynamicBatchSize = props.getBatchSize();  // ex: 10000
        final int minBatchSize = 2000;                // 下限：你可以調
        final int maxBatchSize = props.getBatchSize();// 上限：不超過設定值

        Long slowMsCfg = props.getSlowBatchMs();
        long targetSlowMs = (slowMsCfg != null ? slowMsCfg : 5000L); // 沒設定就用 5 秒
        long targetFastMs = targetSlowMs / 2;                        // 例如 slow=6s => fast=3s

        for (int batch = 1; batch <= props.getMaxBatchesPerRun(); batch++) {
            long t0 = System.currentTimeMillis();

            int affected = doOneBatchTransactional(executor, dynamicBatchSize);
            long cost = System.currentTimeMillis() - t0;

            if (affected <= 0) {
                log.info("[ELT-ADAPT][{}][runId={}] no more rows | totalMoved={} | batch#={} | finalBatchSize={}",
                        kind, runId, totalMoved, batch, dynamicBatchSize);
                break;
            }

            totalMoved += affected;
            double qps = (affected * 1000.0) / Math.max(1, cost);

            log.info("[ELT-ADAPT][{}][runId={}] batch#{} END | affected={} | cost={} ms | ~{}/s | totalMoved={} | batchSize={}",
                    kind, runId, batch, affected, cost, String.format("%.0f", qps), totalMoved, dynamicBatchSize);

            if (cost >= targetSlowMs) {
                log.warn("[ELT-ADAPT][{}][runId={}] SLOW batch | batch#{} | cost={} ms (>= {} ms) | affected={} | batchSize={}",
                        kind, runId, batch, cost, targetSlowMs, affected, dynamicBatchSize);
            }

            // === 核心：根據耗時調整下一批 batchSize ===
            if (cost > targetSlowMs) {
                // 太慢 => 對半砍，但不能小於 minBatchSize
                int old = dynamicBatchSize;
                dynamicBatchSize = Math.max(minBatchSize, dynamicBatchSize / 2);
                if (dynamicBatchSize != old) {
                    log.info("[ELT-ADAPT][{}][runId={}] batch#{} SLOW -> shrink batchSize {} -> {}",
                            kind, runId, batch, old, dynamicBatchSize);
                }
            } else if (cost < targetFastMs && affected == dynamicBatchSize) {
                // 很快 且 有塞滿 => 放大 batchSize（最多到 maxBatchSize）
                int old = dynamicBatchSize;
                dynamicBatchSize = Math.min(maxBatchSize, dynamicBatchSize * 2);
                if (dynamicBatchSize != old) {
                    log.info("[ELT-ADAPT][{}][runId={}] batch#{} FAST -> grow batchSize {} -> {}",
                            kind, runId, batch, old, dynamicBatchSize);
                }
            }

            // 每批之間喘一下
            if (props.getPauseMs() > 0) {
                try {
                    Thread.sleep(props.getPauseMs());
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("[ELT-ADAPT][{}][runId={}] SUMMARY | window=[{}, {}) | totalMoved={}",
                kind, runId, start, end, totalMoved);
        return totalMoved;
    }

    private int doOneBatchTransactional(BatchExecutor executor, int batchSize) {
        TransactionTemplate tt = new TransactionTemplate(txManager);
        tt.setReadOnly(false);
        return tt.execute(status -> executor.execute(batchSize));
    }

    @FunctionalInterface
    public interface BatchExecutor {
        int execute(int batchSize);
    }
}