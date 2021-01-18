package com.fuzs.linearregression;

import java.util.List;

import com.fuzs.fib.KMeansData;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class App {
    public static void main(String args[]) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Double, Double>> lr = env.fromElements(new Tuple2<Double, Double>(1.0, 0.0));
        DataSet<Tuple2<Double, Double>> data = KMeansData.getDefaultPointDataSet(env);
        
        IterativeDataSet<Tuple2<Double, Double>> lrIte = lr.iterate(400);


        DataSet<Tuple2<Double, Double>>  iterBody =  data.map(new calDiff()).withBroadcastSet(lrIte, "lr")
        .reduce(new calSum())
        .map(new calRes()).withBroadcastSet(lr, "lr");



       DataSet<Tuple2<Double, Double>>   res = lrIte.closeWith(iterBody);

       res.print();

    }

    public static final class calDiff extends RichMapFunction<Tuple2<Double, Double>, Tuple3<Double, Double,Long>> {

        private List<Tuple2<Double, Double>> lr;

        @Override
        public void open(Configuration parameters) throws Exception {
            lr = getRuntimeContext().getBroadcastVariable("lr");
        }

        @Override
        public  Tuple3<Double, Double,Long> map(Tuple2<Double, Double> v) throws Exception {
            return new Tuple3(lr.get(0).f0 * v.f0 - v.f1,(lr.get(0).f0 * v.f0 - v.f1)*v.f0,1L);
        }
    }

    public static final class calSum implements ReduceFunction<Tuple3<Double, Double,Long>>{

        @Override
        public Tuple3<Double, Double,Long> reduce(Tuple3<Double, Double,Long> arg0, Tuple3<Double, Double,Long> arg1)
                throws Exception {

            return new Tuple3(arg0.f0 + arg1.f0,arg0.f1 + arg1.f1,arg0.f2+arg1.f2);
        }

    }

    public static final class calRes extends RichMapFunction<Tuple3<Double, Double,Long>, Tuple2<Double, Double>> {

        private List<Tuple2<Double, Double>> lr;

        @Override
        public void open(Configuration parameters) throws Exception {
            lr = getRuntimeContext().getBroadcastVariable("lr");
        }

        @Override
        public Tuple2<Double, Double> map(Tuple3<Double, Double,Long> v) throws Exception {
            return new Tuple2(lr.get(0).f0 - v.f0 * (0.5/v.f2),lr.get(0).f1 - v.f1 * (0.5/v.f2));
        }
    }


}
