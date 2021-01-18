package com.fuzs.pi;

import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class IterativePi {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        //迭代次数
        int iterativeNum=100000;
        Random random=new Random(1);
        IterativeDataSet<Integer> iterativeDataSet=env.fromElements(0).iterate(iterativeNum);

        DataSet<Integer> mapResult=iterativeDataSet.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x=random.nextDouble();
                double y=random.nextDouble();
                value+=(x*x+y*y<=1)?1:0;
                return value;
            }
        });

        //迭代结束的条件
        DataSet<Integer> result=iterativeDataSet.closeWith(mapResult);
        result.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer count) throws Exception {
                return count/(double)iterativeNum*4;
            }
        }).print();        
    }
}
