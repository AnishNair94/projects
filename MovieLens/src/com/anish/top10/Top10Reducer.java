package com.anish.top10;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> TMreduce = new TreeMap<Integer, Text>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

		for (Text value : values) {

			

			String[] field = value.toString().split("::", -1);

			if (field.length == 2) {

				TMreduce.put(Integer.parseInt(field[1]), new Text(value));

				if (TMreduce.size() > 10) {

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