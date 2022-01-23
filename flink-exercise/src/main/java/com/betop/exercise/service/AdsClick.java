package com.betop.exercise.service;

import com.betop.exercise.entity.AdsClickLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * @author long
 * @description: 各省份页面广告点击量实时统计
 * @date 2022/1/23 21:05
 */
public class AdsClick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("flink-exercise/input/AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4])
                            );
                }).map(log -> Tuple2.of(Tuple2.of(log.getProvince(), log.getAdId()), 1L))
        .returns(TUPLE(TUPLE(STRING, LONG),LONG))
        .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                return value.f0;
            }
        }).sum(1)
        .print("省份-广告")
        ;

        env.execute();
    }
}
