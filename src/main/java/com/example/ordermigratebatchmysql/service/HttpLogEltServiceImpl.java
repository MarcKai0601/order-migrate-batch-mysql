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
public class HttpLogEltServiceImpl implements HttpLogEltService {

    private final HttpLogEltMapper orderMapper;
    private final WithdrawHttpLogEltMapper withdrawMapper;
    private final EltProperties props;
    private final PlatformTransactionManager txManager;

    @Override
    public int runOrderBatches(LocalDateTime start, LocalDateTime end, String runId) {
        return runBatches("order", start, end, runId);
    }

    @Override
    public int runWithdrawBatches(LocalDateTime start, LocalDateTime end, String runId) {
        return runBatches("withdraw", start, end, runId);
    }

    @Override
    public int countMissing(String kind, LocalDateTime start, LocalDateTime end) {
        if ("order".equals(kind)) {
            return orderMapper.countMissingForRange(start, end);
        } else if ("withdraw".equals(kind)) {
            return withdrawMapper.countMissingForRange(start, end);
        } else {
            throw new IllegalArgumentException("unknown kind: " + kind);
        }
    }

    /**
     * 在「指定時間窗」內做多批搬移（例如：某一天 00:00~24:00）
     */
    private int runBatches(String kind, LocalDateTime start, LocalDateTime end, String runId) {
        int totalMoved = 0;

        for (int batch = 1; batch <= props.getMaxBatchesPerRun(); batch++) {
            long t0 = System.currentTimeMillis();

            int affected = doOneBatchTransactional(kind, start, end);
            long cost = System.currentTimeMillis() - t0;

            if (affected <= 0) {
                log.info("[ELT][{}][runId={}] no more rows | totalMoved={} | batch#={}",
                        kind, runId, totalMoved, batch);
                break;
            }

            totalMoved += affected;
            double qps = (affected * 1000.0) / Math.max(1, cost);

            log.info("[ELT][{}][runId={}] batch#{} END | affected={} | cost={} ms | ~{}/s | totalMoved={}",
                    kind, runId, batch, affected, cost, String.format("%.0f", qps), totalMoved);

            Long slowMs = props.getSlowBatchMs();
            if (slowMs != null && cost >= slowMs) {
                log.warn("[ELT][{}][runId={}] SLOW batch | batch#{} | cost={} ms (>= {} ms) | affected={}",
                        kind, runId, batch, cost, slowMs, affected);
            }

            // 每批之間稍微喘一下，降低長時間壓力
            if (props.getPauseMs() > 0) {
                try {
                    Thread.sleep(props.getPauseMs());
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("[ELT][{}][runId={}] SUMMARY | window=[{}, {}) | totalMoved={}",
                kind, runId, start, end, totalMoved);
        return totalMoved;
    }

    /**
     * 單批 + TransactionTemplate => 一批一個 tx，不會變成超長交易
     */
    private int doOneBatchTransactional(String kind, LocalDateTime start, LocalDateTime end) {
        TransactionTemplate tt = new TransactionTemplate(txManager);
        tt.setReadOnly(false);
        // 需要可以再加 timeout / 隔離級別:
        // tt.setTimeout(30);
        // tt.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        return tt.execute(status -> {
            if ("order".equals(kind)) {
                return orderMapper.insertMissingForRange(start, end, props.getBatchSize());
            } else {
                return withdrawMapper.insertMissingForRange(start, end, props.getBatchSize());
            }
        });
    }
}