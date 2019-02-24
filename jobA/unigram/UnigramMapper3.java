package unigram;
import java.io.IOException;
        import java.lang.StringBuffer;
        import java.lang.Math;

        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.io.DoubleWritable;


public class UnigramMapper3 extends Mapper<Object, Text, Text, DoubleWritable>{

    private long test = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // access counter
        this.test  = context.getConfiguration().getLong("counter", 0);
    }

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();

        String unigram = line.split("\t")[0];

        String[] mySplit = line.split("\t")[1].split(",");

        double tf = Double.parseDouble(mySplit[1]);
        double fi = Math.log10(this.test/ Double.parseDouble(mySplit[2]));



        double foo = tf * fi;

        context.write(new Text(mySplit[0] + "," + unigram), new DoubleWritable(foo));
    }
}