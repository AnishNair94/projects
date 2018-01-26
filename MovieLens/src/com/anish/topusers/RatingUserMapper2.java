package com.anish.topusers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingUserMapper2 extends Mapper<Object, Text, Text, Text> {

	private Text movieId = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		// Output of First Job
		//18-35::profession::movieid::rating
		
		String[] field = values.toString().split("::", -1);
		if (field != null && field.length == 4) {
			movieId.set(field[2]);
			outvalue.set("C" + field[0] + "::" + field[1]+"::"+field[3]);
			
			//            movieID, C18-35::profession::rating
			context.write(movieId, outvalue);

}

}

}