package com.anish.topusers;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<Object, Text, Text, Text> {

	private Text keyData = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
//       Genre1::18-35::profession::rating
		
		String[] field = values.toString().split("::", -1);
		if (field != null && field.length == 4) {
			keyData.set(field[2] + "::" + field[1]);
			outvalue.set(field[0] + "::" + field[3]);
			
			//    profession::18-35, Genre1::rating          
			context.write(keyData, outvalue);

		}

	}

}