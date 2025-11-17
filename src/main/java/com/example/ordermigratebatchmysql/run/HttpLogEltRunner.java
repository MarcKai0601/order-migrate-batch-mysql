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
@Profile("elt-run")
@RequiredArgsConstructor
public class HttpLogEltRunner implements CommandLineRunner {

    private final HttpLogEltService service;
    private final EltProperties props;

    @Override
    public void run(String... args) {
        String runId = UUID.randomUUID().toString().substring(0, 8);
        ZoneId zone = ZoneId.of(props.getZoneId());

        // 整體窗口：上一個完整月 (例如今天 11/17 => 10/01 00:00 ~ 11/01 00:00)
        LocalDate today = LocalDate.now(zone);
        LocalDate monthStart = today.withDayOfMonth(1).minusMonths(1);
        LocalDate monthEnd = today.withDayOfMonth(1);

        LocalDateTime windowStart = monthStart.atStartOfDay();
        LocalDateTime windowEnd = monthEnd.atStartOfDay();

        log.info("=== [ELT] START | window=[{}, {}) | batchSize={} maxBatches={} | runId={} ===",
                windowStart, windowEnd, props.getBatchSize(), props.getMaxBatchesPerRun(), runId);

        // 整個月的預估缺口（只看 order / withdraw 全窗）
        int orderMissing = service.countMissing("order", windowStart, windowEnd);
        int withdrawMissing = service.countMissing("withdraw", windowStart, windowEnd);
        log.info("[ELT][runId={}] PRECHECK | orderMissing={} | withdrawMissing={}",
                runId, orderMissing, withdrawMissing);

        int orderMovedTotal = 0;
        int withdrawMovedTotal = 0;

        // 🔹關鍵：把「一個月」拆成「一天一天」跑
        for (LocalDate d = monthStart; d.isBefore(monthEnd); d = d.plusDays(1)) {
            LocalDateTime dayStart = d.atStartOfDay();
            LocalDateTime dayEnd = d.plusDays(1).atStartOfDay();

            log.info("[ELT][runId={}] === DAY {} | window=[{}, {}) ===",
                    runId, d, dayStart, dayEnd);

            // 先搬 order
            int movedOrder = service.runOrderBatches(dayStart, dayEnd, runId);
            orderMovedTotal += movedOrder;

            // 再搬 withdraw（如果你希望並行，之後可以把這兩個丟進 Executor）
            int movedWithdraw = service.runWithdrawBatches(dayStart, dayEnd, runId);
            withdrawMovedTotal += movedWithdraw;
        }

        log.info("=== [ELT] DONE | runId={} | orderMoved={} / {} | withdrawMoved={} / {} ===",
                runId, orderMovedTotal, orderMissing, withdrawMovedTotal, withdrawMissing);
    }
}