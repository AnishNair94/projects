package com.anish.topusers;


import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

	// Hash Map to store and retrive professions
private HashMap<Integer, String> profession_mapping = new HashMap<Integer, String>();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
	String[] field = values.toString().split("::", -1);

	if (field !=null && field.length == 5 && field[0].length() > 0) {
        // get profession description
		String profession = profession_mapping.get(Integer.parseInt(field[3]));
		// Getting the age group
		String ageRange   = PickAge.getAge(field[2]);
        
		if (ageRange != null) {	
			//                    userid              U18-35::profession
			context.write(new Text(field[0]), new Text("U" + ageRange + "::" + profession));
		}
	}
	}
@Override
public void setup(Context context) {
	
	// setting up the mapping between the profession Id and Description
	profession_mapping.put(0,"other" );
	profession_mapping.put(1, "academic/educator");
	profession_mapping.put(2, "artist");
	profession_mapping.put(3, "clerical/admin");
	profession_mapping.put(4, "college/grad student");
	profession_mapping.put(5, "customer service");
	profession_mapping.put(6, "doctor/health care");
	profession_mapping.put(7, "executive/managerial");
	profession_mapping.put(8, "farmer");
	profession_mapping.put(9, "homemaker");
	profession_mapping.put(10, "K-12 student");
	profession_mapping.put(11, "lawyer");
	profession_mapping.put(12, "programmer");
	profession_mapping.put(13, "retired");
	profession_mapping.put(14, "sales/marketing");
	profession_mapping.put(15, "scientist");
	profession_mapping.put(16, "self-employed");
	profession_mapping.put(17, "technician/engineer");
	profession_mapping.put(18, "tradesman/craftsman");
	profession_mapping.put(19, "unemployed");
	profession_mapping.put(20, "writer");
	}
}