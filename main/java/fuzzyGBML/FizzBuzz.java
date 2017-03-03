package fuzzyGBML;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class FizzBuzz {
	public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Example01");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.parallelize(IntStream.range(1, 100).boxed().collect(Collectors.toList())).map(x -> {
            if (x % 15 == 0) {
                return "FizzBuzz";
            } else if (x % 5 == 0) {
                return "Fizz";
            } else if (x % 3 == 0) {
                return "Buzz";
            } else {
                return x.toString();
            }
        }).foreach(x -> System.out.println(x));
        sc.stop();
    }
}
