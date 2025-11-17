package com.example.ordermigratebatchmysql.service;

import com.example.ordermigratebatchmysql.config.EltProperties;
import com.example.ordermigratebatchmysql.mapper.HttpLogEltMapper;
import com.example.ordermigratebatchmysql.mapper.WithdrawHttpLogEltMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class HttpLogEltServiceImpl implements HttpLogEltService {

    private final HttpLogEltMapper orderMapper;
    private final WithdrawHttpLogEltMapper withdrawMapper;
    private final EltProperties props;

    @Override
    public int countOrderMissing(LocalDateTime start, LocalDateTime end) {
        return orderMapper.countMissingForRange(start, end);
    }

    @Override
    public int countWithdrawMissing(LocalDateTime start, LocalDateTime end) {
        return withdrawMapper.countMissingForRange(start, end);
    }

    @Override
    @Transactional  // 💡 一批一個 Transaction
    public int runOneOrderBatch(LocalDateTime start, LocalDateTime end) {
        int batchSize = props.getBatchSize();
        int affected = orderMapper.insertMissingForRange(start, end, batchSize);
        log.debug("[ELT][order] batch affected={}", affected);
        return affected;
    }

    @Override
    @Transactional  // 💡 一批一個 Transaction
    public int runOneWithdrawBatch(LocalDateTime start, LocalDateTime end) {
        int batchSize = props.getBatchSize();
        int affected = withdrawMapper.insertMissingForRange(start, end, batchSize);
        log.debug("[ELT][withdraw] batch affected={}", affected);
        return affected;
    }
}