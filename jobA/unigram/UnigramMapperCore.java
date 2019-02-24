package unigram;

import java.io.IOException;
import java.lang.StringBuffer;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class UnigramMapperCore extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String text = value.toString();
        if (text.isEmpty()) return;

        String test[] =  value.toString().split("<====>");
        if(test.length < 3) {
            return;
        }
        String tokenID = value.toString().split("<====>")[1];
        StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);

        String tokenFilter = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
        while(token.hasMoreElements()) {
            String out = token.nextToken();
            // filter tokens
            StringBuffer cleanToken = new StringBuffer();
            for (char element : out.toCharArray()){
                if(tokenFilter.indexOf(element) != -1)
                    cleanToken.append(element);
            }

            if (cleanToken.toString().isEmpty() || token.toString().isEmpty())
            {
                continue;
            }


            String ngram = cleanToken.toString();
            ngram = ngram.replaceAll("[^A-Za-z0-9]","").toLowerCase();


            //System.out.println("first test 1: " + ngram + " 2: " + tokenID);
//
            String testsplit = tokenID + "," + ngram;
            //      System.out.println("secondtest testsplit: " + testsplit);
            String[] mySplit = testsplit.toString().split(",");
            //   System.out.println("thirdtest testsplitarray: " + Arrays.toString(mySplit) + " 2: " + mySplit.length);



            if(!(mySplit.length == 2)){
                System.out.println("mapper failed: -------------------------------------");
                continue;}
            String mykey = mySplit[0];  // ID
            String mykey2 = mySplit[1];  // Word

            String add = mykey + "," + mykey2;


            context.write(new Text(add), one);


        }
        return;
    }
}
