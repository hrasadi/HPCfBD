import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class ReduceMicroBenchmark {

    private static final int MIN_MESSAGE_SIZE = 1;
    private static final int MAX_MESSAGE_SIZE = 1048576; // 2 ^ 20
    private static final int SKIP = 200;
    private static final int ITERATIONS = 1000;

    public static void main(String[] args) {
        //Initializing Spark
        SparkConf conf = new SparkConf().setAppName("ComputationMicroBenchmark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        File output = new File(args[0]);

        PrintWriter writer = null;

        try {
            writer = new PrintWriter(new FileOutputStream(output));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int size = MIN_MESSAGE_SIZE; size * 4 <= MAX_MESSAGE_SIZE; size *= 2) {
            int skip = SKIP;
            int iterations = ITERATIONS;

            float avgPartitions = 0;
            float avgTime = 0;

            for (int i = 0; i < iterations + skip; i++) {


                Float[] arrayOfZeros = new Float[size];
                for (int j = 0; j < size; j++) {
                    arrayOfZeros[j] = 0f;
                }

                List<Float> listOfZeros = Arrays.asList(arrayOfZeros);

                JavaRDD<Float> listOfOnesRDD = sc.parallelize(listOfZeros);

                int partitions = listOfOnesRDD.partitions().size();

                listOfOnesRDD.cache();

                //Start time
                long start_time = System.currentTimeMillis();

                listOfOnesRDD.reduce(new Function2<Float, Float, Float>() {
                    public Float call(Float a, Float b) {
                        return a + b;
                    }
                });

                //End time
                long end_time = System.currentTimeMillis();

                if (i >= skip) {
                    avgTime += end_time - start_time;
                    avgPartitions += partitions;
                }
            }

            if (writer != null) {
                writer.println("Size: " + size * 4 +
                        " Partitions: " + avgPartitions / iterations +
                        " The elapsed time: " + (avgTime) * 1.0 / 1000
                );
            }
        }

        if (writer != null) {
            writer.close();
        }

        //Stopping the server and clearing the routes
        sc.stop();


    }
}
