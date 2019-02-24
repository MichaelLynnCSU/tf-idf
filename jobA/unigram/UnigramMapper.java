package unigram;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.io.Text;


public class UnigramMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();
        if (line.isEmpty()) return;


        String[] mySplit = line.split("\t");

        String mykey = mySplit[0].split(",")[0]; // ID
        String mykey2 = mySplit[0].split(",")[1]; // word
        String mykey3 = mySplit[1]; // value
        String add = mykey2 + "," + mykey3;
        context.write(new Text(mykey), new Text(add));
        }
        }