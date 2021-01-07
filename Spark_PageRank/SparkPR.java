/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package SparkExamples;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
public final class SparkPR {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  static void showWarning() {
    String warning = "WARN: This is a naive implementation of PageRank " +
            "and is given as an example! \n" +
            "Please use the PageRank implementation found in " +
            "org.apache.spark.graphx.lib.PageRank for more conventional use.";
    System.err.println(warning);
  }

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
      System.exit(1);
    }

    showWarning();

    SparkConf sparkConf = new SparkConf().setAppName("SparkPR");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
	long start = System.currentTimeMillis();
    
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    int partition = Integer.parseInt(args[3]);
    
    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        parts[0] = parts[0].toString().replaceAll("AAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZ", "");
        parts[1] = parts[1].toString().replaceAll("AAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZAAAAAAAAAZ", "");
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).groupByKey(partition).persist(StorageLevel.MEMORY_AND_DISK());
    
    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < Integer.parseInt(args[1]); current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
        	
          @Override
          public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
            int urlCount = Iterables.size(s._1);
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1) {
              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
            }
            return results.iterator();
          }
      });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }
    
    ranks.foreach(p -> System.out.println(p));
    //ranks.saveAsTextFile("ranks.txt");
    
    
    long end = System.currentTimeMillis();
    System.out.println("running time " + (end - start) / 1000 + "s");

    String results = "running time " + (new Double((end - start) / 1000)).toString() + "s"+"input file : "+args[0]+", iteration : "+args[1];
    System.out.println(results);
    String outputurl = ".//results.txt";
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputurl,true));
    writer.write(results);	
    writer.newLine();
    writer.close();

    ctx.stop();
  }
}
