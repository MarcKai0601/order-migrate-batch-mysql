package com.example.ordermigratebatchmysql.mapper;

import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

public interface WithdrawHttpLogEltMapper {

    /** 單批搬移：回傳受影響筆數 */
    int insertMissingForRange(@Param("start") LocalDateTime start,
                              @Param("end") LocalDateTime end,
                              @Param("batchSize") int batchSize);

    /** 可選：查估計數（只做觀察用） */
    int countMissingForRange(@Param("start") LocalDateTime start,
                             @Param("end") LocalDateTime end);
}