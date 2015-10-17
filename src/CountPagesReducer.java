import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gokul on 10/12/15.
 */
public class CountPagesReducer extends MapReduceBase implements Reducer<LongWritable, IntWritable, Text, NullWritable> {
    @Override
    public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
        long count = 0;
        while(values.hasNext()){
            count++;
            values.next();
        }
        output.collect(new Text("N="+Long.toString(count)), NullWritable.get());
    }
}
