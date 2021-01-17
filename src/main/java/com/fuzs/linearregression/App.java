package com.fuzs.linearregression;

import java.util.List;

import com.fuzs.fib.KMeansData;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class App {
    public static void main(String args[]) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Double, Double>> lr = env.fromElements(new Tuple2<Double, Double>(1.0, 0.0));
        DataSet<Tuple2<Double, Double>> data = KMeansData.getDefaultPointDataSet(env);
        IterativeDataSet<Tuple2<Double, Double>> lrIte = lr.iterate(4);

        DataSet<Tuple2<Double, Double>>  iterBody =  data.map(new calDiff()).withBroadcastSet(lrIte, "lr")
        .reduce(new calSum())
        .map(new calRes()).withBroadcastSet(lr, "lr");

       DataSet<Tuple2<Double, Double>>   res = lrIte.closeWith(iterBody);

       res.print();

    }

    public static final class calDiff extends RichMapFunction<Tuple2<Double, Double>, Tuple2<Double, Double>> {

        private List<Tuple2<Double, Double>> lr;

        @Override
        public void open(Configuration parameters) throws Exception {
            lr = getRuntimeContext().getBroadcastVariable("lr");
        }

        @Override
        public Tuple2<Double, Double> map(Tuple2<Double, Double> v) throws Exception {
            return new Tuple2(lr.get(0).f0 * v.f0 - v.f1,(lr.get(0).f0 * v.f0 - v.f1)*v.f0);
        }
    }

    public static final class calSum implements ReduceFunction<Tuple2<Double, Double>>{

        @Override
        public Tuple2<Double, Double> reduce(Tuple2<Double, Double> arg0, Tuple2<Double, Double> arg1)
                throws Exception {

            return new Tuple2(arg0.f0 + arg1.f0,arg0.f1 + arg1.f1);
        }

    }

    public static final class calRes extends RichMapFunction<Tuple2<Double, Double>, Tuple2<Double, Double>> {

        private List<Tuple2<Double, Double>> lr;

        @Override
        public void open(Configuration parameters) throws Exception {
            lr = getRuntimeContext().getBroadcastVariable("lr");
        }

        @Override
        public Tuple2<Double, Double> map(Tuple2<Double, Double> v) throws Exception {
            return new Tuple2(lr.get(0).f0 - v.f0 * (1/80),lr.get(0).f1 - v.f1 * (1/80));
        }
    }


}
