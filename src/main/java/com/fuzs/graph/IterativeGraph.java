package com.fuzs.graph;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class IterativeGraph {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int iterativeNum = 100;
        DataSet<Long> vertix = env.fromElements(1L,2L,3L,4L,5L,6L,7L);

        DataSet<Tuple2<Long,Long>> edges = env.fromElements(
            Tuple2.of(1L, 2L),
            Tuple2.of(2L, 3L),
            Tuple2.of(2L, 4L),
            Tuple2.of(3L, 4L),
            Tuple2.of(5L, 6L),
            Tuple2.of(5L, 7L),
            Tuple2.of(6L, 7L)
        );
        edges=edges.flatMap(new FlatMapFunction<Tuple2<Long,Long>, Tuple2<Long,Long>>() {
            @Override
            public void flatMap(Tuple2<Long, Long> tuple, Collector<Tuple2<Long, Long>> collector) throws Exception {
                collector.collect(tuple);
                collector.collect(Tuple2.of(tuple.f1,tuple.f0));
            }
        });
        //initialSolutionSet，将顶点映射为(vertix,vertix)的形式
        DataSet<Tuple2<Long,Long>> initialSolutionSet=vertix.map(new MapFunction<Long, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Long vertix) throws Exception {
                return Tuple2.of(vertix,vertix);
            }
        });
        //initialWorkSet
        DataSet<Tuple2<Long,Long>> initialWorkSet=vertix.map(new MapFunction<Long, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Long vertix) throws Exception {
                return Tuple2.of(vertix,vertix);
            }
        });
        //第一个字段做迭代运算
        DeltaIteration<Tuple2<Long,Long>,Tuple2<Long,Long>> iterative=
        initialSolutionSet.iterateDelta(initialWorkSet,iterativeNum,0);
                //数据集合边做join操作，然后求出当前顶点的邻居顶点的最小ID值
        DataSet<Tuple2<Long,Long>> changes=iterative.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
                .groupBy(0).aggregate(Aggregations.MIN,1)
                //和solution set进行join操作，更新solution set，如果当前迭代结果中的最小ID小于solution中的ID值，则发送到下一次迭代运算中继续运算，否则不发送
                .join(iterative.getSolutionSet()).where(0).equalTo(0)
                .with(new ComponetIDFilter());
        //关闭迭代计算
        DataSet<Tuple2<Long,Long>> result=iterative.closeWith(changes,changes);
        result.print();
    }

    public static class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long,Long>,Tuple2<Long,Long>,Tuple2<Long,Long>>{

        @Override
        public Tuple2<Long, Long> join(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2) throws Exception {
            return Tuple2.of(t2.f1,t1.f1);
        }
    }

    public static class ComponetIDFilter implements FlatJoinFunction<Tuple2<Long,Long>,Tuple2<Long,Long>,Tuple2<Long,Long>> {

        @Override
        public void join(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2, Collector<Tuple2<Long, Long>> collector) throws Exception {
            if(t1.f1<t2.f1){
                collector.collect(t1);
            }
        }
    }
}
