package com.example.ordermigratebatchmysql.run;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.service.HttpLogEltService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("elt-run") // 只在指定 profile 啟動
@RequiredArgsConstructor
public class HttpLogEltRunner implements CommandLineRunner {

    private final HttpLogEltService service;
    private final EltProperties props;

    @Override
    public void run(String... args) {
        try {
            service.migratePrevAndThisMonthLoop(
                    props.getBatchSize(), props.getMaxBatchesPerRun(), props.getPauseMs());
        } catch (Exception e) {
            log.error("order 任務失敗，但繼續執行 withdraw 任務", e);
        }

        try {
            service.migratePrevAndThisMonthLoopbyWithdraw(
                    props.getBatchSize(), props.getMaxBatchesPerRun(), props.getPauseMs());
        } catch (Exception e) {
            log.error("withdraw 任務失敗", e);
        }
    }
}
