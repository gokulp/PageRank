import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gokul on 10/13/15.
 */
public class SortPageByRankReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    public static String numberOfPagesStr;

    public void configure(JobConf jobConf){
        numberOfPagesStr = jobConf.get("numberOfPages");
    }

    @Override
    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double numberOfPages = Double.parseDouble(numberOfPagesStr);
        double lowerThreshold = 0.0003;
        double rank = Double.parseDouble(key.toString());
        if ( rank < lowerThreshold )
            return;

        while(values.hasNext()) output.collect(values.next(), key);
    }
}
