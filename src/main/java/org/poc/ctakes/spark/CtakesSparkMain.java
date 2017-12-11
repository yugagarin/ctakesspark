/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.poc.ctakes.spark;

//Java imports
import java.io.File;
import java.io.IOException;
import java.util.List;

//Hadoop imports
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.*;

//Apache Spark imports
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.SparkFiles;

/**
 * @author Yuriy Toropov
 *
 */
public class CtakesSparkMain {

	/**
	 * @param args
	 *            input command-line parameters
	 */
	public static void main(String[] args) {
		// args[0] holds file name
		
		int numPartitions=4; //could be done as a parameter args[1]
		
		SparkConf conf = new SparkConf();
		conf.setAppName("ctakes-demo");
		// conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		//dependencies should be all set by now...

		JavaRDD<String> note = sc.textFile("adl:///tmp/testdata100.txt");
		//JavaRDD<String> output = note.map(new CtakesFunction());
		
		//repartition RDD for processing parallelism
		note=note.repartition(numPartitions);
		JavaRDD<String> output = note.mapPartitions(new CtakesFlatMapFunction());

		// save output to hdfs
		output.saveAsTextFile("adl:///tmp/testdata100.out/");

		sc.close();
	}

}
