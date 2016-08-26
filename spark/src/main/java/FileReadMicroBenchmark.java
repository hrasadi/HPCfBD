/*
 * HPCfBD Benchmark - COPYRIGHT 2016 IACS
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 *
 * Created by hamid on 3/13/16.
 */
public class FileReadMicroBenchmark {
    //        int partitionsNum = new Integer(args[2]);
    public static void main(String[] args) {

        //Start time
        long start_time = System.currentTimeMillis();

        //File pointer
        String logFile = args[0];
        //Initializing Spark
        SparkConf conf = new SparkConf().setAppName("FileReadMicroBenchmark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Creating RDD from a file and setting number of tasks
        JavaRDD<String> lines = sc.textFile(logFile);

        //lines.sample(false, 0.01);
        JavaRDD<String> post_1 = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {

                return false;
            }
        });


        post_1.count();

        //End time
        long end_time = System.currentTimeMillis();


        File output = new File(args[1]);

        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(output));

            //Elapsed time for execution
            writer.println("The elapsed time: " + (end_time - start_time) * 1.0 / 1000);

            writer.close();
        } catch (FileNotFoundException e) {

            e.printStackTrace();
        }


        //Stopping the server and clearing the routes
        sc.stop();
    }
}
