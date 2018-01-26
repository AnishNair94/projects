package com.anish.top10;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Top10Mapper extends Mapper<Object, Text, NullWritable, Text> {
	// TreeMap will store number of views as Key and views::MovieName as Value
	// Using TreeMap , for easy sorting
	
private TreeMap<Integer, Text> TMovie = new TreeMap<Integer, Text>();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {



	String[] field = values.toString().split("::", -1);

	if (field != null && field.length == 2) {
        // Taking the total views
		int count = Integer.parseInt(field[1]);
       
		//inserting (number of views, numberofviews::moviename) 
		TMovie.put(count, new Text(field[0]+"::"+field[1]));
        
		// logic to keep only last 10 of each mapper i.e Top 10 
		if (TMovie.size() > 10) {

			TMovie.remove(TMovie.firstKey());

		}

	}
}

@Override
protected void cleanup(Context context) throws IOException, InterruptedException {

	for (Map.Entry<Integer, Text> entry : TMovie.entrySet()) {

		context.write(NullWritable.get(), entry.getValue());

	}
}

}