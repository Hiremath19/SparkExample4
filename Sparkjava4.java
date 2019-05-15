import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Sparkjava4 {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparktraining");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        Tuple2<Integer, Integer> t1 = new Tuple2<>(0, 0);
        Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>> seqfun = new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Integer j) throws Exception {
                Integer sum = t1._1 + j;
                Integer count = t1._2 + 1;
                return new Tuple2<Integer, Integer>(sum, count);

            }


        };
        Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> combiner = new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                Integer sum = t1._1 + t2._1;
                Integer count = t1._2 + t2._2;
                return new Tuple2<>(sum, count);
            }
        };

        Tuple2<Integer,Integer> result =rdd1.aggregate(t1,seqfun,combiner);
        System.out.println(result);

        float avg=result._1 / (float) result._2;

        System.out.println("Average: " + avg);



    }


}