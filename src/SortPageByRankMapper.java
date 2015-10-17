import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by gokul on 10/13/15.
 */
public class SortPageByRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
        String strValues = value.toString();
        String[] parts = strValues.split("\t");

        output.collect(new DoubleWritable(Double.parseDouble(parts[1])), new Text(parts[0]));
    }
}
