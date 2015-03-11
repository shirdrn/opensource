package org.shirdrn.spark.job;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public final class JavaWordCount {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaWordCount <master> <file>");
			System.exit(1);
		}

		JavaSparkContext ctx = new JavaSparkContext(args[0], "JavaWordCount", System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaWordCount.class));
		JavaRDD<String> lines = ctx.textFile(args[1], 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, Integer> ones = words.map(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1 + ": " + tuple._2);
		}
		System.exit(0);
	}
}
