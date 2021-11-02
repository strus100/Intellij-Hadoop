import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, CustomOutputValue> {

        private final static CustomOutputValue one = new CustomOutputValue(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (((LongWritable) key).get() != 0) {
                String[] line = value.toString().split(",");

                String filmId = line[1];
                String rate = line[3];

                context.write(new Text(filmId), new Text(rate));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, CustomOutputValue, Text, CustomOutputValue> {
        private CustomOutputValue result = new CustomOutputValue();

        public void reduce(Text filmId, Iterable<IntWritable> rates,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : rates) {
                sum += val.get();
                count++;
            }
            result.set(sum + count + "");
            context.write(filmId, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomOutputValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}