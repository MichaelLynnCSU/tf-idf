package unigram;
import java.io.IOException;
        import java.util.Map;
        import java.util.HashMap;
        import java.util.LinkedHashMap;
        import java.util.List;
        import java.util.ArrayList;

        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;


public class UnigramReducer4 extends Reducer<Text, Text, Text, Text>{


    private <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

        List<Map.Entry<K, V>> l = new ArrayList<>(map.entrySet());

        l.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        Map<K, V> myr = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : l) {

            myr.put(entry.getKey(), entry.getValue());
        }

        return myr;
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String, Double> ds = new HashMap<String, Double>();
        for (Text t : values) {

            String value = t.toString();

            String[] mysplit = value.split("<=>");

            String sentence = mysplit[0];

            Double s = Double.parseDouble(mysplit[1]);

            ds.put(sentence, s);
        }


        Map<String, Double> sd = this.sortByValue(ds);

        // output top 3 from every article
        int go = 0;

        for (Map.Entry<String,Double> entry : sd.entrySet()) {

            go += 1;

            context.write(key, new Text(entry.getKey()));

            if (go >= 3) break;

        }
    }
}




