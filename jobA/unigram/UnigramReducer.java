package unigram;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.Text;

public class UnigramReducer extends Reducer<Text, Text, Text, Text>{

Set<String> occ = new HashSet<String>();

    public void reduce(Text key, Iterable<Text> values, Context context
    ) throws IOException, InterruptedException {

        List<String> vals = new ArrayList<String>();

        for (Text tvalue : values)
            vals.add(tvalue.toString());


        int max = -1;
        for (String sum : vals) {
            int count = Integer.parseInt(sum.split(",")[1]);

            if (max < count)
                max = count;

        }


        if (!occ.contains(key.toString())){
            occ.add(key.toString());
            // name and value of counter
            context.getCounter("counter", "ok").increment(1); //Increment counters
        }


        for (String value : vals) {
            String[] mySplit = value.split(",");
            int sum = Integer.parseInt(mySplit[1]);
            double tf = 0.5 + 0.5 * (sum / (double)max);
            context.write(key, new Text(value +  "," + Double.toString(tf)));
        }
    }
}