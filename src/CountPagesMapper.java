import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by gokul on 10/12/15.
 */
public class CountPagesMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
        output.collect(new LongWritable(1), new IntWritable(1));
    }
}
