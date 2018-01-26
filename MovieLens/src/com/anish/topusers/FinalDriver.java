package com.anish.topusers;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FinalDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 6) {
	      System.err.println("Usage: topusers <in> <in> <out> <out> <in> <out>");
	      System.exit(2);
	    }
	    Job job1 = Job.getInstance(conf);

/*
args[0]  = ratings.dat
args[1]  = users.dat
args[2]  = path of first job output
args[3]  = path of second job output
args[4]  = movies.dat
args[5]  = path of final output

yarn jar /home/hduser/eclipse-workspace/MovieLens/topusers.jar com.anish.topusers.FinalDriver
 /movie-data/ratings.dat /movie-data/users.dat /topusers/out1 /topusers/out2 /movie-data/movies.dat /topusers/finalout */



job1.setJarByClass(FinalDriver.class);
job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");

TextOutputFormat.setOutputPath(job1, new Path(args[2]));
job1.setOutputKeyClass(Text.class);
job1.setOutputValueClass(Text.class);

job1.setReducerClass(RatingUserJoin.class);

MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);

int code = job1.waitForCompletion(true) ? 0 : 1;
int code2 = 1;
if (code == 0) {

Job job2 = Job.getInstance(conf);
job2.setJarByClass(FinalDriver.class);
job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");

TextOutputFormat.setOutputPath(job2, new Path(args[3]));
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(Text.class);

job2.setReducerClass(MovieRUJoin.class);

MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class,
RatingUserMapper2.class);
MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, MovieMapper.class);
code2 = job2.waitForCompletion(true) ? 0 : 1;

}

if (code2 == 0) {

Job job = Job.getInstance(conf);
job.setJarByClass(FinalDriver.class);
job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
job.setJobName("Final Task");

FileInputFormat.addInputPath(job, new Path(args[3]));
FileOutputFormat.setOutputPath(job, new Path(args[5]));

job.setMapperClass(FinalMapper.class);
job.setReducerClass(FinalReducer.class);

job.setNumReduceTasks(1);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}
}