package com.betop.exercise.service;

import com.betop.exercise.entity.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author long
 * @description:
 * @date 2022/1/22 11:13
 */
public class PageView_WordCount_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("flink-exercise/input/UserBehavior.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                }).filter(behavior -> "pv".equals(behavior.getBehavior())) // c. 选择需要输出的数据
        .map(behavior -> Tuple2.of("pv", 1L))
        .returns(Types.TUPLE(Types.STRING, Types.LONG))
        .keyBy(value -> value.f0) // 4. 指定 key 分组
        .sum(1) // 5. 计算总和
        .print()
        ;
        env.execute();
    }
}
