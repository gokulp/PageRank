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
public class AdjGraphCreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String link = value.toString();
        String source = link.substring(0, link.indexOf("\t"));
        String destination = link.substring(link.indexOf("\t")+1);

        output.collect(new Text(source), new Text(destination));
    }
}
