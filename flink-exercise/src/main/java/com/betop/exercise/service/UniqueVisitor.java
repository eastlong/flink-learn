package com.betop.exercise.service;

import com.betop.exercise.entity.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.util.HashSet;


/**
 * @author long
 * @description: 网站独立访客数（UV）的统计：到底有多少不同的用户访问了网站？
 * @date 2022/1/23 16:53
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("flink-exercise/input/UserBehavior.csv").flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = line.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4]));
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(Tuple2.of("uv", userBehavior.getUserId()));// 根据userId来区分不同的用户
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    HashSet<Long> userIds = new HashSet<>();// 用户的集合，可以去重
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context context, Collector<Integer> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect(userIds.size());
                    }
                }).print("uv");

        env.execute();
    }
}
