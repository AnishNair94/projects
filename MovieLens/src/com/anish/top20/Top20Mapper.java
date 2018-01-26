package com.anish.top20;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Top20Mapper extends Mapper<Object, Text, NullWritable, Text> {
	// TreeMap will store number of average rating as Key and averagerating::MovieName as Value
	// Using TreeMap , for easy sorting
	
private TreeMap<Double, Text> TMovie = new TreeMap<Double, Text>();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {



	String[] field = values.toString().split("::", -1);

	if (field != null && field.length == 2) {
        // Taking the average rating
		double avgrate = Double.parseDouble(field[1]);
       
		//inserting (average rating, avgrating::moviename) 
		TMovie.put(avgrate, new Text(field[0]+"::"+field[1]));
        
		// logic to keep only last 20 of each mapper i.e Top 20 
		if (TMovie.size() > 20) {

			TMovie.remove(TMovie.firstKey());

		}

	}
}

@Override
protected void cleanup(Context context) throws IOException, InterruptedException {

	for (Map.Entry<Double, Text> entry : TMovie.entrySet()) {

		context.write(NullWritable.get(), entry.getValue());

	}
}

}