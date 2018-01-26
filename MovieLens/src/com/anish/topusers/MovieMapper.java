package com.anish.topusers;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<Object, Text, Text, Text> {

	private Text movieId = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		//MovieID::Title::Genres
		String[] field = values.toString().split("::", -1);
		if (field != null && field.length == 3 && field[0].length() > 0) {
			movieId.set(field[0]);
			outvalue.set("M" + field[1] + "::" + field[2]);
			
			//           MovieID , MTitle::Genres
			context.write(movieId, outvalue);

		}
	}
}
