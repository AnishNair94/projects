package com.anish.top10;


import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MovieRatingJoin extends Reducer<Text, Text, Text, Text> {

// using list to store movie and rating
private ArrayList<Text> listofmovie = new ArrayList<Text>();
private ArrayList<Text> listofrating = new ArrayList<Text>();

@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	listofmovie.clear();

	listofrating.clear();

	for (Text text : values) {

		if (text.charAt(0) == 'M') {

			listofmovie.add(new Text(text.toString().substring(1))); //Taking out Movie Name

		} else if (text.charAt(0) == 'R') {

			listofrating.add(new Text(text.toString().substring(1))); //Taking out rating 

		}

	}

    mrInnerJoin(context);
}

private void mrInnerJoin(Context context) throws IOException, InterruptedException {
if (!listofmovie.isEmpty() && !listofrating.isEmpty()) {
for (Text moviesData : listofmovie) {
	//        moviename::number of times it was rated    
	//  set output delimiter as :: in driver
context.write(moviesData, new Text(String.valueOf(listofrating.size())));
}
}
}
}