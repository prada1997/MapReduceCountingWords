package edu.rmit.cosc2367.s3764267;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Task3 {

    private static final Logger LOG = Logger.getLogger(Task3.class);

    //TokenizerMapper implements mapper interface.
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

       //map method is a in-mapper combiner mapper, It will store the words and their number of occurrences in the hash map.
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Setting log-level to debugging.
            LOG.setLevel(Level.DEBUG);
            LOG.debug("The Mapper Task of Pradhuman, s3764267");

            StringTokenizer itr = new StringTokenizer(value.toString());
            //creating hash map store the words and its number of time occurrences.
            Map<String,Integer> wordList = new HashMap<String,Integer>();

            while (itr.hasMoreTokens()) {

                //storing the word in the string variable.
                String token = itr.nextToken();

                //if hashmap contains the word given in the token.
                if(wordList.containsKey(token)) {

                    //increase the count of the given word by 1.
                    int sum = wordList.get(token) + 1;
                    wordList.put(token, sum);
                }
                else {
                    //if hashmap does not contain the word given in the token, then add the word in the hashmap.
                    wordList.put(token, 1);
                }
            }

            for(Map.Entry<String,Integer> index : wordList.entrySet()) {
                context.write(new Text(index.getKey()), new IntWritable(index.getValue()));
            }

        }
    }


    //IntSumReducer implements reducer interface.
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {

            // Setting log-level to debugging
            LOG.setLevel(Level.DEBUG);
            LOG.debug("The Reducer Task of Pradhuman, s3764267");

            int sum = 0;

            for (IntWritable val : values) {
                //counting the occurrence of the same words.
                sum += val.get();
            }

            result.set(sum);
            //storing the final words count of the same words.
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //creating default configuration.
        Configuration conf = new Configuration();
        //creating job using config and name.
        Job job = Job.getInstance(conf, "Task 3");
        job.setJarByClass(Task3.class);

        // Setting log-level to information.
        LOG.setLevel(Level.INFO);

        // Log all the arguments passed to the application
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);

        //starting mapper class for the job.
        job.setMapperClass(TokenizerMapper.class);
        //starting combiner class for the job.
        job.setCombinerClass(IntSumReducer.class);
        //starting reducer class for the job.
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //giving input path of the file.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //giving path for output file.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //exit program after job complete.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
