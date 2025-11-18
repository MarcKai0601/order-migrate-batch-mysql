package com.example.ordermigratebatchmysql.run;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.service.HttpLogEltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class HttpLogEltRunner implements CommandLineRunner {

    private final HttpLogEltService service;
    private final EltProperties props;

    @Override
    public void run(String... args) {
        String runId = UUID.randomUUID().toString().substring(0, 8);
        ZoneId zone = ZoneId.of(props.getZoneId());

        // 整體窗口：從「上一個月 1 號 00:00」到「現在」
        LocalDate today = LocalDate.now(zone);
        LocalDate monthStart = today.withDayOfMonth(1).minusMonths(1);

        LocalDateTime windowStart = monthStart.atStartOfDay();
        LocalDateTime windowEnd = LocalDateTime.now(zone);

        log.info("=== [ELT] START | window=[{}, {}) | batchSize={} maxBatches={} | runId={} ===",
                windowStart, windowEnd, props.getBatchSize(), props.getMaxBatchesPerRun(), runId);

        // 整體預估缺口（只看 order / withdraw 全窗）
        int orderMissing = service.countMissing("order", windowStart, windowEnd);
        int withdrawMissing = service.countMissing("withdraw", windowStart, windowEnd);
        log.info("[ELT][runId={}] PRECHECK | orderMissing={} | withdrawMissing={}",
                runId, orderMissing, withdrawMissing);

        int orderMovedTotal = 0;
        int withdrawMovedTotal = 0;

        // 🔹把「一個月」拆成「一天一天」跑
        for (LocalDate d = monthStart; d.isBefore(today); d = d.plusDays(1)) {
            LocalDateTime dayStart = d.atStartOfDay();
            LocalDateTime dayEnd = d.plusDays(1).atStartOfDay();

            // 先看這一天的缺口大小
            int dayOrderMissing = service.countMissing("order", dayStart, dayEnd);
            int dayWithdrawMissing = service.countMissing("withdraw", dayStart, dayEnd);

            // 🔍 預估這一天會需要幾個 batch
            int estOrderBatches = (int) Math.ceil(dayOrderMissing / (double) props.getBatchSize());
            int estWithdrawBatches = (int) Math.ceil(dayWithdrawMissing / (double) props.getBatchSize());

            log.info("[ELT][runId={}] === DAY {} | window=[{}, {}) | dayOrderMissing={} (estBatches={}) | dayWithdrawMissing={} (estBatches={}) ===",
                    runId, d, dayStart, dayEnd,
                    dayOrderMissing, estOrderBatches,
                    dayWithdrawMissing, estWithdrawBatches);

            boolean useHalfDayByVolume =
                    dayOrderMissing > props.getHalfDaySwitchThreshold()
                            || dayWithdrawMissing > props.getHalfDaySwitchThreshold();

            boolean useHalfDayByBatchCount =
                    estOrderBatches > props.getMaxBatchesBeforeSplit()
                            || estWithdrawBatches > props.getMaxBatchesBeforeSplit();

            boolean useHalfDay = useHalfDayByVolume || useHalfDayByBatchCount;

            if (!useHalfDay) {
                // ✅ 正常情況：整天搬一次
                log.info("[ELT][runId={}] DAY {} use FULL-DAY window (useHalfDayByVolume={} useHalfDayByBatchCount={})",
                        runId, d, useHalfDayByVolume, useHalfDayByBatchCount);

                int movedOrder = service.runOrderBatches(dayStart, dayEnd, runId);
                orderMovedTotal += movedOrder;

                int movedWithdraw = service.runWithdrawBatches(dayStart, dayEnd, runId);
                withdrawMovedTotal += movedWithdraw;
            } else {
                // 🚨 資料量太大或預估 batch 過多：切成「半天半天」搬，降低一次查詢/交易壓力
                LocalDateTime half1Start = dayStart;
                LocalDateTime half1End = dayStart.plusHours(12);
                LocalDateTime half2Start = half1End;
                LocalDateTime half2End = dayEnd;

                log.warn("[ELT][runId={}] DAY {} LARGE or MANY-BATCH day, use HALF-DAY windows | "
                                + "threshold={} | maxBatchesBeforeSplit={} | orderMissing={} (estBatches={}) | withdrawMissing={} (estBatches={})",
                        runId, d,
                        props.getHalfDaySwitchThreshold(),
                        props.getMaxBatchesBeforeSplit(),
                        dayOrderMissing, estOrderBatches,
                        dayWithdrawMissing, estWithdrawBatches);

                // 🔹 上半天
                int movedOrderH1 = processWindowWithHourFallback(
                        "H1", runId, d, half1Start, half1End);
                int movedWithdrawH1 = processWindowWithHourFallbackForWithdraw(
                        "H1", runId, d, half1Start, half1End);

                // 🔹 下半天
                int movedOrderH2 = processWindowWithHourFallback(
                        "H2", runId, d, half2Start, half2End);
                int movedWithdrawH2 = processWindowWithHourFallbackForWithdraw(
                        "H2", runId, d, half2Start, half2End);

                orderMovedTotal += (movedOrderH1 + movedOrderH2);
                withdrawMovedTotal += (movedWithdrawH1 + movedWithdrawH2);
            }
        }

        log.info("=== [ELT] DONE | runId={} | orderMoved={} / {} | withdrawMoved={} / {} ===",
                runId, orderMovedTotal, orderMissing, withdrawMovedTotal, withdrawMissing);
    }

    /**
     * 半天窗口（order）：「如果這半天缺口太大」或「預估批次數太多」就切成一小時一小時搬，否則整個半天一次搬完。
     */
    private int processWindowWithHourFallback(String label,
                                              String runId,
                                              LocalDate day,
                                              LocalDateTime winStart,
                                              LocalDateTime winEnd) {

        int missing = service.countMissing("order", winStart, winEnd);
        int estBatches = (int) Math.ceil(missing / (double) props.getBatchSize());

        log.info("[ELT][order][runId={}] DAY {} {} | window=[{}, {}) | missing={} (estBatches={})",
                runId, day, label, winStart, winEnd, missing, estBatches);

        boolean useHourlyByVolume = missing > props.getHourSwitchThreshold();
        boolean useHourlyByBatchCount = estBatches > props.getMaxBatchesBeforeSplit();
        boolean useHourly = useHourlyByVolume || useHourlyByBatchCount;

        if (!useHourly) {
            // ✅ 半天一次就好
            log.info("[ELT][order][runId={}] DAY {} {} use HALF-DAY window directly "
                            + "(useHourlyByVolume={} useHourlyByBatchCount={})",
                    runId, day, label, useHourlyByVolume, useHourlyByBatchCount);
            return service.runOrderBatches(winStart, winEnd, runId);
        }

        // 🚨 半天還是太大或預估批次過多：切成一小時一小時搬
        log.warn("[ELT][order][runId={}] DAY {} {} VERY LARGE or MANY-BATCH half-day, use HOURLY windows | "
                        + "hourThreshold={} | maxBatchesBeforeSplit={} | missing={} (estBatches={})",
                runId, day, label,
                props.getHourSwitchThreshold(),
                props.getMaxBatchesBeforeSplit(),
                missing, estBatches);

        int totalMoved = 0;
        for (LocalDateTime t = winStart; t.isBefore(winEnd); t = t.plusHours(1)) {
            LocalDateTime hourStart = t;
            LocalDateTime hourEnd = t.plusHours(1);
            if (hourEnd.isAfter(winEnd)) {
                hourEnd = winEnd;
            }

            log.info("[ELT][order][runId={}] DAY {} {} HOUR | window=[{}, {})",
                    runId, day, label, hourStart, hourEnd);

            int moved = service.runOrderBatches(hourStart, hourEnd, runId);
            totalMoved += moved;
        }
        return totalMoved;
    }

    /**
     * 半天窗口（withdraw）：「如果這半天缺口太大」或「預估批次數太多」就切成一小時一小時搬，否則整個半天一次搬完。
     */
    private int processWindowWithHourFallbackForWithdraw(String label,
                                                         String runId,
                                                         LocalDate day,
                                                         LocalDateTime winStart,
                                                         LocalDateTime winEnd) {

        int missing = service.countMissing("withdraw", winStart, winEnd);
        int estBatches = (int) Math.ceil(missing / (double) props.getBatchSize());

        log.info("[ELT][withdraw][runId={}] DAY {} {} | window=[{}, {}) | missing={} (estBatches={})",
                runId, day, label, winStart, winEnd, missing, estBatches);

        boolean useHourlyByVolume = missing > props.getHourSwitchThreshold();
        boolean useHourlyByBatchCount = estBatches > props.getMaxBatchesBeforeSplit();
        boolean useHourly = useHourlyByVolume || useHourlyByBatchCount;

        if (!useHourly) {
            // ✅ 半天一次就好
            log.info("[ELT][withdraw][runId={}] DAY {} {} use HALF-DAY window directly "
                            + "(useHourlyByVolume={} useHourlyByBatchCount={})",
                    runId, day, label, useHourlyByVolume, useHourlyByBatchCount);
            return service.runWithdrawBatches(winStart, winEnd, runId);
        }

        // 🚨 半天還是太大或預估批次過多：切成一小時一小時搬
        log.warn("[ELT][withdraw][runId={}] DAY {} {} VERY LARGE or MANY-BATCH half-day, use HOURLY windows | "
                        + "hourThreshold={} | maxBatchesBeforeSplit={} | missing={} (estBatches={})",
                runId, day, label,
                props.getHourSwitchThreshold(),
                props.getMaxBatchesBeforeSplit(),
                missing, estBatches);

        int totalMoved = 0;
        for (LocalDateTime t = winStart; t.isBefore(winEnd); t = t.plusHours(1)) {
            LocalDateTime hourStart = t;
            LocalDateTime hourEnd = t.plusHours(1);
            if (hourEnd.isAfter(winEnd)) {
                hourEnd = winEnd;
            }

            log.info("[ELT][withdraw][runId={}] DAY {} {} HOUR | window=[{}, {})",
                    runId, day, label, hourStart, hourEnd);

            int moved = service.runWithdrawBatches(hourStart, hourEnd, runId);
            totalMoved += moved;
        }
        return totalMoved;
    }
}