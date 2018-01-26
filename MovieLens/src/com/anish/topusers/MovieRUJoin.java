package com.anish.topusers;


import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRUJoin extends Reducer<Text, Text, Text, Text> {

	private ArrayList<Text> listMovies = new ArrayList<Text>();
	private ArrayList<Text> listRating = new ArrayList<Text>();
	private Text outvalue=new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		listMovies.clear();
		listRating.clear();

		for (Text text : values) {
           
			if (text.charAt(0) == 'M') {
//	                                       MTitle::Genres|Genres|Genres
				listMovies.add(new Text(text.toString().substring(1)));
			} else if (text.charAt(0) == 'C') {
				//                      18-35::profession::rating
				listRating.add(new Text(text.toString().substring(1)));
			}

		}

		mrJoinLogic(context);

}

private void mrJoinLogic(Context context) throws IOException, InterruptedException {

if (!listMovies.isEmpty() && !listRating.isEmpty()) {
	
	for (Text moviesData : listMovies) {
		//MTitle::Genres|Genres|Genres
		String[] data = moviesData.toString().split("::");
		
		//Genres1|Genres2|Genres3
		String[] genres=data[1].split("\\|");

		for (Text ratingData : listRating) {

			for (String genre : genres) {
				outvalue.set(genre);
			//    	          Genre1      18-35::profession::rating
				context.write(outvalue, ratingData);
			}

		}
	}
	
}
}

}