package com.fuzs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class IteratePi {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(100);

        DataSet<Integer> iteration = initial.map(new MapFunction<Integer,Integer>(){

			@Override
			public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
				return i + ((x * x + y * y < 1) ? 1 : 0);
            }});
            DataSet<Integer> count = initial.closeWith(iteration);
            
            count.map(new MapFunction<Integer, Double>(){
            

				@Override
				public Double map(Integer count) throws Exception {
                    return count /(double) 10000 * 4;
				}
             }).print();

            env.execute();
    }
}
