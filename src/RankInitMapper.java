import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by gokul on 10/13/15.
 *
 * This mapper collects output from Job 2 which created adjacency graph for each page without RedLinks.
 * For Each page it separates title and outlinks and sends to Reducer routine.
 *
 */
public class RankInitMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public static String numberOfPagesStr;

    public void configure(JobConf jobConf){
        numberOfPagesStr = jobConf.get("numberOfPages");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String adjMap = value.toString();
        //String[] links = adjMap.split("\t");
        String title = adjMap.substring(0,adjMap.indexOf("\t"));
        String outlinks = adjMap.substring(1 + adjMap.indexOf("\t"));

        output.collect(new Text(title), new Text(outlinks));
    }
}
