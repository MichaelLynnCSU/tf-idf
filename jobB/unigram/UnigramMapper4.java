package unigram;
import java.io.IOException;
        import java.lang.StringBuffer;
        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.util.Map;
        import java.util.HashMap;
        import java.util.LinkedHashMap;
        import java.util.List;
        import java.util.ArrayList;

        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;


public class UnigramMapper4 extends Mapper<Object, Text, Text, Text>{

    private Map<String, Double> a = new HashMap<String, Double>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        try{
            Path[] cacheFiles = context.getLocalCacheFiles();

            if(cacheFiles != null && cacheFiles.length > 0) {

                for(Path cachefile : cacheFiles) {

                    readFile(cachefile);

                }

            }

        } catch(IOException ex) {

            System.err.println("error: " + ex.getMessage());

        }

    }

    private void readFile(Path filePath) {
        try{

            String fileName = filePath.toString().substring(5);

            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));

            String ln;
            // parse lines
            while((ln = bufferedReader.readLine()) != null) {

                String[] mysplit = ln.split("\t");

                String mykey = mysplit[0];

                double v = Double.parseDouble(mysplit[1]);

                a.put(mykey, v);
            }
        } catch(IOException ex) {

            System.err.println("error: " + ex.getMessage());

        }
    }

    // sort the mao by value and more into hashmap
    private <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        Map<K, V> myresult = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            myresult.put(entry.getKey(), entry.getValue());
        }

        return myresult;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();
        if (line.isEmpty()) return;

        String[] mysplit = line.split("<====>");

        if(mysplit.length < 3) {
            return;
        }

        // filter ngrams
        String filter = "abcdefghijklmnopqrstuvwxyz1234567890";

        // split into mykeys
        String mykey = mysplit[1];
        String text = mysplit[2];

        for (String sentence : text.split("\\.")) {

            // trim whitespaces from parse
            if (sentence.trim().isEmpty()) continue;

            double stc = 0.0;

            Map<String, Double> token_value = new HashMap<String, Double>();

            for (String word : sentence.split(" ")){

                StringBuffer sb = new StringBuffer();

                // get the sentence score
                double ws = 0.0;

                for (char c : word.toCharArray()){

                    char t2 = Character.toLowerCase(c);

                    if(filter.indexOf(t2) != -1)

                        sb.append(t2);
                }

                if (sb.toString().isEmpty()) continue;

                if (!token_value.containsKey(sb.toString())) continue;

                // set scores
                ws = a.get(mykey + "," + sb.toString());

                token_value.put(sb.toString(), ws);

            }
            Map<String, Double> st = this.sortByValue(token_value);


            int go = 0;

            for (Map.Entry<String,Double> entry : st.entrySet()) {

                go += 1;

                // sum sentence score
                stc += entry.getValue();

                if (go >= 5) break;
            }
            context.write(new Text(mykey), new Text(sentence + "<=>" + Double.toString(stc)));
        }

    }
}