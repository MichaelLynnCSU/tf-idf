package unigram;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class UnigramReducer2 extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values, Context context
		) throws IOException, InterruptedException {

		List<String> vals = new ArrayList<>();

		for (Text sum : values)
			vals.add(sum.toString());

		int count = 0;
		for (String value : vals)
			count++;
		
		for (String value : vals)
			context.write(key, new Text(value.toString() + "," + Integer.toString(count)));
	}
}