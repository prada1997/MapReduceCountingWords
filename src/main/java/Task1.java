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

public class Task1 {

    private static final Logger LOG = Logger.getLogger(Task1.class);

    public static void main(String[] args) throws Exception {

        //creating default configuration.
        Configuration conf = new Configuration();
        //creating job using config and name.
        Job job = Job.getInstance(conf, "Task 1");
        job.setJarByClass(Task1.class);

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
        private Text smallWords = new Text("Short Words");
        private Text mediumWords = new Text("Medium Words");
        private Text longWords = new Text("Long Words");
        private Text extraLongWords = new Text("Extra Long Words");

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

                //checking the object read from while is a word or not.
                if( initialAlphabet > 'A' && initialAlphabet < 'Z') {

                    //storing the length of the word.
                    int letterLength = word.getLength();

                    if (letterLength > 0 && letterLength < 5) {
//                        LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which is a " + smallWords + ".");

                        //Writing 1 value in the small words category.
                        context.write(smallWords, one);
                    }
                    else if (letterLength > 4 && letterLength < 8) {
//                        LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which is a " + mediumWords + ".");

                        //Writing 1 value in the meadium words category.
                        context.write(mediumWords, one);
                    }
                    else if (letterLength > 7 && letterLength < 11) {
//                        LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which is a " + longWords + ".");

                        //Writing 1 value in the long words category.
                        context.write(longWords, one);
                    }
                    else if (letterLength > 10) {
//                        LOG.debug("The Mapper Task of Pradhuman, s3764267: The word is " + word + " which is a " + extraLongWords + ".");

                        //Writing 1 value in the extra large words category.
                        context.write(extraLongWords, one);
                    }
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
