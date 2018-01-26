package com.anish.top10;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MovieMapper extends Mapper<Object, Text, Text, Text> {

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {

String[] field = values.toString().split("::", -1);

// Movieid::MovieName::Genre

if (field != null  && field.length == 3 && field[0].length() > 0) {

	// using flag M to identify if it is from movies.dat
context.write(new Text(field[0]), new Text("M"+field[1]));
}
}
}