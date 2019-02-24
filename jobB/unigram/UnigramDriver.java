package unigram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;



public class UnigramDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Unigram");
        job.setJarByClass(UnigramDriver.class);

        job.addCacheFile(new Path("/testLoc/output4/part-r-00000").toUri());


        job.setMapperClass(UnigramMapper4.class);
        job.setReducerClass(UnigramReducer4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/testLoc/PA2Dataset/"));
        FileOutputFormat.setOutputPath(job, new Path("/testLoc/output5"));

       System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
