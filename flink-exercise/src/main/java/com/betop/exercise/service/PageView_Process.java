package com.betop.exercise.service;

import com.betop.exercise.entity.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author long
 * @description:
 * @date 2022/1/22 11:13
 */
public class PageView_Process {
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
        }).filter(behavior -> "pv".equals(behavior.getBehavior()))
        .keyBy(UserBehavior::getBehavior)
        .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
            long count = 0;
            @Override
            public void processElement(UserBehavior userBehavior, Context context, Collector<Long> out) throws Exception {
                count ++;
                out.collect(count);
            }
        }).print();

        //  执行任务
        env.execute();
    }
}
