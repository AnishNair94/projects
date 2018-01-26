package com.anish.topusers;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, GRank> map = new HashMap<String, GRank>();

	private TreeMap<Double, String> finalMap = new TreeMap<Double, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//	                    profession::18-35, <Genre1::rating><Genre1::rating>   
		map.clear();
		finalMap.clear();

		for (Text text : values) {
			//Genre1::rating
			String[] value = text.toString().split("::");
			String genre = value[0];
			double rating = Double.parseDouble(value[1]);
           
			// logic to sum and count of Genre
			GRank ranking = map.get(genre);

			if (ranking != null) {
				ranking.setSum(ranking.getSum() + rating);
				ranking.setCount(ranking.getCount() + 1);
			} else {
				GRank rankingNew = new GRank();
				rankingNew.setSum(rating);
				rankingNew.setCount(1);
				map.put(genre, rankingNew);
			}

		}
		
		for (Map.Entry<String, GRank> entry : map.entrySet()) {
			GRank gr = entry.getValue();
			//taking average
			double average = gr.getSum() / gr.getCount();
			
			// inserting average and genre
			finalMap.put(average, entry.getKey());
		}

		StringBuilder sb = new StringBuilder();
		int count = 0;

		// sort descending by average 
		for (Map.Entry<Double, String> entry : finalMap.descendingMap().entrySet()) {
			//Film-Noir::War::Animation::Musical::Drama::
			if (count < 5) {

				sb.append(entry.getValue() + "::");
				count++;
			} else {
				break;
			}

		}
//                  profession::18-35  , Film-Noir::War::Animation::Musical::Drama
		context.write(key, new Text(sb.toString().substring(0, sb.toString().lastIndexOf("::"))));

	}

	}
