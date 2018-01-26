package com.anish.topusers;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<Object, Text, Text, Text> {

@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {

	// userid::movieid::rating::time
	String[] field = values.toString().split("::", -1);

	if (field != null  && field.length == 4 && field[0].length() > 0) {
       //                 userid               Rmovieid::rating
	context.write(new Text(field[0]), new Text("R"+field[1]+ "::" +field[2] ));
	}
	}
}