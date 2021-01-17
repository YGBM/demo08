package com.fuzs.fib;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;

public class App {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Double,Double>>  data = KMeansData.getDefaultPointDataSet(env);
        DataSet<Tuple2<Double,Double>> center = KMeansData.getDefaultPointDataSet(env);
        
        IterativeDataSet<Tuple2<Double, Double>> dataIte = data.iterate(10);
        
        DataSet addTen = dataIte.map(new MapFunction<Tuple2<Double, Double>,Tuple2<Double, Double>>(){

			@Override
			public Tuple2<Double, Double> map(Tuple2<Double, Double> arg0) throws Exception {
				
				return new Tuple2(arg0.f1,arg0.f1+arg0.f0);
			}});
           DataSet result =  dataIte.closeWith(addTen);

           result.print();

           //env.execute();
    }
}
