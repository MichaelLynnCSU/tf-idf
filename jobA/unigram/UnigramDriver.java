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
        job.setNumReduceTasks(30);
        job.setMapperClass(UnigramMapperCore.class);
        job.setReducerClass(UnigramReducerCore.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/testLoc/PA2Dataset/"));
        FileOutputFormat.setOutputPath(job, new Path("/testLoc/output"));


        job.waitForCompletion(true);

//----------------------------------------------------
      Job job2 = Job.getInstance(conf, "Unigram2");
      job2.setJarByClass(UnigramDriver.class);
        job2.setNumReduceTasks(30);
      job2.setMapperClass(UnigramMapper.class);
      job2.setReducerClass(UnigramReducer.class);

       job2.setMapOutputKeyClass(Text.class);
       job2.setMapOutputValueClass(Text.class);

     FileInputFormat.addInputPath(job2, new Path("/testLoc/output/"));
     FileOutputFormat.setOutputPath(job2, new Path("/testLoc/output2"));
      job2.waitForCompletion(true);


        Counter counter = job2.getCounters().findCounter("counter","ok");

//------------------------------------------------------------------------------
       Job job3 = Job.getInstance(conf, "Unigram3");
       job3.setJarByClass(UnigramDriver.class);
        job3.setNumReduceTasks(30);

       job3.setMapperClass(UnigramMapper2.class);
       job3.setReducerClass(UnigramReducer2.class);

       job3.setMapOutputKeyClass(Text.class);
       job3.setMapOutputValueClass(Text.class);

       FileInputFormat.addInputPath(job3, new Path("/testLoc/output2/"));
       FileOutputFormat.setOutputPath(job3, new Path("/testLoc/output3"));
        job3.waitForCompletion(true);

//--------------------------------------------------------------------------------
        Job job4 = Job.getInstance(conf, "Unigram4");
        job4.getConfiguration().setLong("counter", counter.getValue());
        job4.setJarByClass(UnigramDriver.class);

        job4.setMapperClass(UnigramMapper3.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job4, new Path("/testLoc/output3/"));
        FileOutputFormat.setOutputPath(job4, new Path("/testLoc/output4"));

       System.exit(job4.waitForCompletion(true) ? 0 : 1);

    }
}
