package com.anish.top20;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top20Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Double, Text> TMreduce = new TreeMap<Double, Text>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

		for (Text value : values) {

			

			String[] field = value.toString().split("::", -1);

			if (field.length == 2) {

				TMreduce.put(Double.parseDouble(field[1]), new Text(value));

				if (TMreduce.size() > 20) {

					TMreduce.remove(TMreduce.firstKey());

				}

			}

		}
 
		//reversing the order of key
		for (Text t : TMreduce.descendingMap().values()) {

			context.write(NullWritable.get(), t);

		}

	}
}