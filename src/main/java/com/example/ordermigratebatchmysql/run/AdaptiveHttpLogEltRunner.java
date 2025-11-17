package com.example.ordermigratebatchmysql.run;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.service.AdaptiveHttpLogEltService;
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
@Profile("elt-run-adaptive")
@RequiredArgsConstructor
public class AdaptiveHttpLogEltRunner implements CommandLineRunner {

    private final AdaptiveHttpLogEltService adaptiveService;
    private final EltProperties props;

    @Override
    public void run(String... args) {
        String runId = UUID.randomUUID().toString().substring(0, 8);
        ZoneId zone = ZoneId.of(props.getZoneId());

        LocalDate today = LocalDate.now(zone);
        LocalDate monthStart = today.withDayOfMonth(1).minusMonths(1);

        LocalDateTime windowStart = monthStart.atStartOfDay();
        LocalDateTime windowEnd = LocalDateTime.now(zone);

        log.info("=== [ELT-ADAPT] START | window=[{}, {}) | runId={} ===",
                windowStart, windowEnd, runId);

        int orderMissing = adaptiveService.countMissing("order", windowStart, windowEnd);
        int withdrawMissing = adaptiveService.countMissing("withdraw", windowStart, windowEnd);
        log.info("[ELT-ADAPT][runId={}] PRECHECK | orderMissing={} | withdrawMissing={}",
                runId, orderMissing, withdrawMissing);

        int orderMovedTotal = 0;
        int withdrawMovedTotal = 0;

        int halfDayThreshold = props.getHalfDaySwitchThreshold();
        int hourThreshold = props.getHourSwitchThreshold();

        // 一天一天往前掃
        for (LocalDate d = monthStart; d.isBefore(today); d = d.plusDays(1)) {
            LocalDateTime dayStart = d.atStartOfDay();
            LocalDateTime dayEnd = d.plusDays(1).atStartOfDay();

            int dayOrderMissing = adaptiveService.countMissing("order", dayStart, dayEnd);
            int dayWithdrawMissing = adaptiveService.countMissing("withdraw", dayStart, dayEnd);

            log.info("[ELT-ADAPT][runId={}] === DAY {} | window=[{}, {}) | dayOrderMissing={} | dayWithdrawMissing={} ===",
                    runId, d, dayStart, dayEnd, dayOrderMissing, dayWithdrawMissing);

            boolean useHalfDay =
                    dayOrderMissing > halfDayThreshold
                            || dayWithdrawMissing > halfDayThreshold;

            if (!useHalfDay) {
                // ✅ 正常：整天一個 window 搬
                log.info("[ELT-ADAPT][runId={}] DAY {} use FULL-DAY window", runId, d);
                orderMovedTotal += adaptiveService.runOrderBatches(dayStart, dayEnd, runId);
                withdrawMovedTotal += adaptiveService.runWithdrawBatches(dayStart, dayEnd, runId);
            } else {
                // 🚨 資料量偏大：先拆成「半天」，再看要不要拆到「每小時」
                LocalDateTime half1Start = dayStart;
                LocalDateTime half1End = dayStart.plusHours(12);
                LocalDateTime half2Start = half1End;
                LocalDateTime half2End = dayEnd;

                log.warn("[ELT-ADAPT][runId={}] DAY {} LARGE volume, use HALF-DAY windows | threshold={} | orderMissing={} | withdrawMissing={}",
                        runId, d, halfDayThreshold, dayOrderMissing, dayWithdrawMissing);

                // 上半天
                orderMovedTotal += processHalfWindow(runId, d, "H1",
                        half1Start, half1End, hourThreshold);

                withdrawMovedTotal += processHalfWindowWithdraw(runId, d, "H1",
                        half1Start, half1End, hourThreshold);

                // 下半天
                orderMovedTotal += processHalfWindow(runId, d, "H2",
                        half2Start, half2End, hourThreshold);

                withdrawMovedTotal += processHalfWindowWithdraw(runId, d, "H2",
                        half2Start, half2End, hourThreshold);
            }
        }

        log.info("=== [ELT-ADAPT] DONE | runId={} | orderMoved={} / {} | withdrawMoved={} / {} ===",
                runId, orderMovedTotal, orderMissing, withdrawMovedTotal, withdrawMissing);
    }

    /**
     * 半天 window for order：
     * - 如果半天缺口 <= hourThreshold => 直接半天 window 搬
     * - 否則 => 再拆成每小時 window 搬
     */
    private int processHalfWindow(String runId,
                                  LocalDate day,
                                  String label,
                                  LocalDateTime halfStart,
                                  LocalDateTime halfEnd,
                                  int hourThreshold) {

        int halfOrderMissing = adaptiveService.countMissing("order", halfStart, halfEnd);

        log.info("[ELT-ADAPT][runId={}] DAY {} {} | ORDER | window=[{}, {}) | halfOrderMissing={}",
                runId, day, label, halfStart, halfEnd, halfOrderMissing);

        if (halfOrderMissing <= hourThreshold) {
            log.info("[ELT-ADAPT][runId={}] DAY {} {} | ORDER use HALF-DAY window", runId, day, label);
            return adaptiveService.runOrderBatches(halfStart, halfEnd, runId);
        }

        // 半天還是太多 => 切成每小時
        log.warn("[ELT-ADAPT][runId={}] DAY {} {} | ORDER too large, split into HOURLY windows | threshold={} | missing={}",
                runId, day, label, hourThreshold, halfOrderMissing);

        int moved = 0;
        LocalDateTime cursor = halfStart;
        while (cursor.isBefore(halfEnd)) {
            LocalDateTime next = cursor.plusHours(1);
            if (next.isAfter(halfEnd)) {
                next = halfEnd;
            }

            log.info("[ELT-ADAPT][runId={}] DAY {} {} | ORDER HOUR window=[{}, {})",
                    runId, day, label, cursor, next);

            moved += adaptiveService.runOrderBatches(cursor, next, runId);
            cursor = next;
        }
        return moved;
    }

    /**
     * 半天 window for withdraw（邏輯跟 order 一樣，獨立統計）
     */
    private int processHalfWindowWithdraw(String runId,
                                          LocalDate day,
                                          String label,
                                          LocalDateTime halfStart,
                                          LocalDateTime halfEnd,
                                          int hourThreshold) {

        int halfWithdrawMissing = adaptiveService.countMissing("withdraw", halfStart, halfEnd);

        log.info("[ELT-ADAPT][runId={}] DAY {} {} | WITHDRAW | window=[{}, {}) | halfWithdrawMissing={}",
                runId, day, label, halfStart, halfEnd, halfWithdrawMissing);

        if (halfWithdrawMissing <= hourThreshold) {
            log.info("[ELT-ADAPT][runId={}] DAY {} {} | WITHDRAW use HALF-DAY window", runId, day, label);
            return adaptiveService.runWithdrawBatches(halfStart, halfEnd, runId);
        }

        log.warn("[ELT-ADAPT][runId={}] DAY {} {} | WITHDRAW too large, split into HOURLY windows | threshold={} | missing={}",
                runId, day, label, hourThreshold, halfWithdrawMissing);

        int moved = 0;
        LocalDateTime cursor = halfStart;
        while (cursor.isBefore(halfEnd)) {
            LocalDateTime next = cursor.plusHours(1);
            if (next.isAfter(halfEnd)) {
                next = halfEnd;
            }

            log.info("[ELT-ADAPT][runId={}] DAY {} {} | WITHDRAW HOUR window=[{}, {})",
                    runId, day, label, cursor, next);

            moved += adaptiveService.runWithdrawBatches(cursor, next, runId);
            cursor = next;
        }
        return moved;
    }
}