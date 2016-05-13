import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;

import java.io.*;
import java.lang.String;

public class AnswersCount {

    public static void main(String[] args) {
        //Start time
        long start_time = System.currentTimeMillis();
        //File pointer
        String logFile = args[0];
        //Initializing Spark
        SparkConf conf = new SparkConf().setAppName("AnswersCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Creating RDD from a file and setting number of tasks
        JavaRDD<String> lines = sc.textFile(logFile);

        //RDD of elements having PostType=1
        JavaRDD<String> post_1 = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {

                String post = "PostTypeId=\"";
                Integer index_post = s.indexOf(post);

                if (index_post != -1) {

                    Integer post_length = post.length();
                    Integer value_post = Integer.parseInt(Character.toString(s.charAt(index_post + post_length)));

                    if (value_post == 1) {

                        return true;

                    }
                }

                return false;
            }
        });
        post_1.cache();
        //RDD of elements as number of answers per question
        JavaRDD<Integer> num_ans = post_1.map(new Function<String, Integer>() {
            public Integer call(String s) {

                int i;
                String ans = "AnswerCount=\"";
                Integer index_ans = s.indexOf(ans);

                if (index_ans != -1) {
                    Integer ans_length = ans.length();
                    String val1 = "";

                    for (i = index_ans + ans_length; s.charAt(i) != '"'; i++) {
                        val1 = val1 + Character.toString(s.charAt(i));
                    }
                    return Integer.parseInt(val1);
                }
                return 0;
            }
        });

        //Average answer per question
        Double average;

        //Action on each element of RDD
        int sum = num_ans.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {

                return a + b;

            }
        });

        //Action on number of elements with PostTypeId=1
        long quesCount = post_1.count();

        //Calculating the Average
        average = sum * 1.0 / quesCount;

        //End time
        long end_time = System.currentTimeMillis();

        File output = new File(args[1]);

        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(output));

            //Printing the results
            writer.println("Sum: " + sum + " and Average: " + average);

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
