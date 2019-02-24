package unigram;

import java.io.IOException;
import java.lang.StringBuffer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class UnigramMapper2 extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
		String line = value.toString();


        String mykey = line.split("\t")[0]; // id
        String[] mySplit = line.split("\t")[1].split(",");

    	context.write(new Text(mySplit[0]), new Text(mykey + "," + mySplit[2]));
	}
}