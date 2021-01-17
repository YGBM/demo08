package com.fuzs;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;

/** 迭代的数据流向：DataStream → IterativeStream → DataStream
 * 以下代码以流开始并连续应用迭代体。大于0的元素将被发送回反馈（feedback）通道，继续迭代，其余元素将向下游转发，离开迭代。
    IterativeStream<Long> iteration = initialStream.iterate();
    DataStream<Long> iterationBody = iteration.map (do something);
    DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
        @Override
        public boolean filter(Long value) throws Exception {
            return value > 0;
        }
    });//这里设置feedback这个数据流是被反馈的通道，只要是value>0的数据都会被重新迭代计算。
    iteration.closeWith(feedback);
    DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
        @Override
        public boolean filter(Long value) throws Exception {
            return value <= 0;
        }
    });
*/
public class KmeanTask {
    public static void main(String[] args) throws Exception {
            final ParameterTool params = ParameterTool.fromArgs(args);

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            // env.setParallelism(2);
            env.getConfig().setGlobalJobParameters(params);

            // 获取输入的数据点和聚类中心，如果路径中有数据就读文件，否则取默认数据
            DataSet<Point> points = getPointDataSet(params, env);
            DataSet<Centroid> centroids = getCentroidDataSet(params, env);
            points.print();
            centroids.print();
            // 设置 K-Means算法的迭代次数
            IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                    // 为每个点(point)计算最近的聚类中心
                    .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                    // 每个聚类中心的点坐标的计数和求和
                    .map(new CountAppender())
                    .groupBy(0)
                    .reduce(new CentroidAccumulator())
                    // 从点计数和坐标，计算新的聚类中心
                    .map(new CentroidAverager());

            // 将新的中心点放到下一次迭代中,closeWith代表最后一次迭代
            DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
            // 最后将分类和聚类的点生成元组
            DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                    // 将point分派到最后聚类中
                    .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

            // 将结果集存到csv文件中或者打印到控制台
            if (params.has("output")) {
                clusteredPoints.writeAsCsv(params.get("output"), "\n", StringUtils.SPACE).setParallelism(1);

                // since file sinks are lazy, we trigger the execution explicitly
                env.execute("KMeans Example");
            } else {
                System.out.println("Printing result to stdout. Use --output to specify output path.");
                clusteredPoints.print();
            }
        }


        // *************************************************************************
        //     自定义函数
        // *************************************************************************

        /** 从数据点确定最近的聚类中心. */
        @FunctionAnnotation.ForwardedFields("*->1")
        public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
            private List<Centroid> centroids;

            /** 从广播变量中读取聚类中心值到集合中. */
            @Override
            public void open(Configuration parameters) throws Exception {
                this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            }

            @Override
            public Tuple2<Integer, Point> map(Point p) throws Exception {

                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;

                // 检查所有的聚类中心
                for (Centroid centroid : centroids) {
                    // 计算每个点与聚类中心的距离（欧式距离）
                    double distance = p.euclideanDistance(centroid);

                    // 满足条件更新最近的聚类中心Id
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroidId = centroid.id;
                    }
                }

                // 生成一个包含聚类中心id和数据点的元组tuple.
                return new Tuple2<>(closestCentroidId, p);
            }
        }

        /** 向tupel2追加计数变量. */
        @FunctionAnnotation.ForwardedFields("f0;f1")
        public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

            @Override
            public Tuple3<Integer/*id*/, Point, Long/*1L*/> map(Tuple2<Integer, Point> t) {
                return new Tuple3<>(t.f0, t.f1, 1L);
            }
        }

        /** 求同一个类所有点的x,y坐标总数和计数点坐标. */
        //@FunctionAnnotation.ForwardedFields("0")
        public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

            @Override
            public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
                return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
            }
        }

        /** 从坐标和点的个数计算新的聚类中心. */
        //@FunctionAnnotation.ForwardedFields("0->id")
        public static final class CentroidAverager implements MapFunction<Tuple3<Integer/*id*/, Point/*累加的坐标点*/, Long/*个数*/>, Centroid> {

            @Override
            public Centroid map(Tuple3<Integer, Point, Long> value) {
                return new Centroid(value.f0, value.f1.div(value.f2));
            }
        }
        
        // *************************************************************************
        //     数据源读取 (数据点和聚类中心)
        // *************************************************************************

        private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
            DataSet<Centroid> centroids;
            if (params.has("centroids")) {
                centroids = env.readCsvFile(params.get("centroids"))
                        .fieldDelimiter(StringUtils.SPACE)
                        .pojoType(Centroid.class, "id", "x", "y");
            } else {
                System.out.println("执行 K-Means 用默认的中心数据集合.");
                System.out.println("Use --centroids to specify file input.");
                // centroids = KMeansData.getDefaultCentroidDataSet(env);
                centroids = null;
            }
            return centroids;
        }

        private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
            DataSet<Point> points;
            if (params.has("points")) {
                // read points from CSV file
                points = env.readCsvFile(params.get("points"))
                        .fieldDelimiter(StringUtils.SPACE)
                        .pojoType(Point.class, "x", "y");
            } else {
                System.out.println("Executing K-Means example with default point data set.");
                System.out.println("Use --points to specify file input.");
                // points = KMeansData.getDefaultPointDataSet(env);
                points = null;
            }
            return points;
        }

    // *************************************************************************
    //    数据类型，POJO内部类
    // *************************************************************************

    /**
     * 简单的二维点.
     */
    public static class Point implements Serializable {

        public double x, y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        }


        public void clear() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + StringUtils.SPACE + y;
        }
    }

    /**
     * 简单的二维中心，包括ID的点
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    }


