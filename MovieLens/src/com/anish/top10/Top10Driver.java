package com.anish.top10;


import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Top10Driver {
public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: top10 <in> <in> <out> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf);

/* delete the output directory before running the job */
FileUtils.deleteDirectory(new File(args[2]));
FileUtils.deleteDirectory(new File(args[3]));

job.setJarByClass(Top10Driver.class);

job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");

TextOutputFormat.setOutputPath(job, new Path(args[2]));

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

job.setReducerClass(MovieRatingJoin.class);

MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

int code = job.waitForCompletion(true) ? 0 : 1;

if (code == 0) {
Job job2 = Job.getInstance(conf);

job2.setJarByClass(Top10Driver.class);

job2.setJobName("Top10");

job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");

FileInputFormat.addInputPath(job2, new Path(args[2]));

FileOutputFormat.setOutputPath(job2, new Path(args[3]));

job2.setMapperClass(Top10Mapper.class);
job2.setReducerClass(Top10Reducer.class);

job2.setNumReduceTasks(1);

job2.setOutputKeyClass(NullWritable.class);
job2.setOutputValueClass(Text.class);

System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}
}


