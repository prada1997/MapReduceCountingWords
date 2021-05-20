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
import java.util.StringTokenizer;

public class Task2 {

    private static final Logger LOG = Logger.getLogger(Task2.class);

    public static void main(String[] args) throws Exception {

        //creating default configuration
        Configuration conf = new Configuration();
        //creating job using config and name
        Job job = Job.getInstance(conf, "Task 2");
        job.setJarByClass(Task2.class);

        // Setting log-level to information.
        LOG.setLevel(Level.INFO);

        // Log all the arguments passed to the application.
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


    //TokenizerMapper implements mapper interface.
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        //text object variable to store word from the file.
        private Text word = new Text();

        //creating categories for the word to count.
        private Text vowels = new Text("Vowels");
        private Text consonant = new Text("Consonants");

        //mapper function will read all words and store them in different categories with its value.
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Setting log-level to debugging.
            LOG.setLevel(Level.DEBUG);
            LOG.debug("The Mapper Task of Pradhuman, s3764267");

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                //storing the word in the String variable.
                String letter = itr.nextToken();
                word.set(letter);
                letter = letter.toUpperCase();
                char initialAlphabet = letter.charAt(0);

                //checking the word starts with vowels or not.
                if (initialAlphabet == 'A' || initialAlphabet == 'E' || initialAlphabet == 'I' || initialAlphabet == 'O' || initialAlphabet == 'U') {
//                    LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which starts with " + vowels + ".");

                    //Writing 1 value in the vowels category.
                    context.write(vowels, one);
                }
                else if ((initialAlphabet > 'A' && initialAlphabet < 'E') || (initialAlphabet > 'E' && initialAlphabet < 'I') || (initialAlphabet > 'I' && initialAlphabet < 'O')
                        || (initialAlphabet > 'O' && initialAlphabet < 'U') || (initialAlphabet > 'U' && initialAlphabet <= 'Z')) {
//                    LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which starts with " + consonant + ".");

                    //Writing 1 value in the small consonants category.
                    context.write(consonant, one);
                }

            }
        }
    }


    //IntSumReducer implements reducer interface.
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        //collecting the data of same categories and counting the data for each category.
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Setting log-level to debugging.
            LOG.setLevel(Level.DEBUG);
            LOG.debug("The Reducer Task of Pradhuman, s3764267");

            int sum = 0;

            for (IntWritable val : values) {

                //counting the words of same categories.
                sum += val.get();
            }

            result.set(sum);
            //storing the final words count of the categories.
            context.write(key, result);
        }
    }
}
