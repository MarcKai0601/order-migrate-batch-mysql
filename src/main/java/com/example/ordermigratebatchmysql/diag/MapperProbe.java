package com.example.ordermigratebatchmysql.diag;

import com.example.ordermigratebatchmysql.mapper.HttpLogEltMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MapperProbe {
    private final HttpLogEltMapper mapper;

    @PostConstruct
    public void print() {
        log.info("Mapper bean: {}", mapper.getClass());
    }
}