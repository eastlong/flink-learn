package com.betop.exercise.service;

import com.betop.exercise.entity.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
public class PageView_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> readTextFile = env.readTextFile("flink-exercise/input/UserBehavior.csv");

        // 转化为JavaBean并过滤出PV
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                // 封装javaBean对象
                UserBehavior userBehavior = new UserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4]));
                // c. 选择需要输出的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(new Tuple2<>("PV", 1));
                }
            }
        });
        // 4. 指定 key 分组
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = pv.keyBy(data -> data.f0);
        // 5. 计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        // 6. 打印输出
        result.print();
        // 7. 执行任务
        env.execute();
    }
}
