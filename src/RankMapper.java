import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by gokul on 10/13/15.
 * This Mapper routine will send outlinks for the current page and voteValue for all the respective outlinks.
 * Which then collected in mapper routine and used to calculate a page Rank.
 *
 */
public class RankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public static String numberOfPagesStr;

    public void configure(JobConf jobConf){
        numberOfPagesStr = jobConf.get("numberOfPages");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String strValue = value.toString();
        String[] links = strValue.split("\t");

        String pageTitle = links[0];
        String rankStr = links[1];
        double rank = Double.parseDouble(rankStr);

        String outlinks = "";
        double voteValueOfNode = 0.0;
        long numOutlinks = links.length - 2;

        if( numOutlinks > 0){
            outlinks = strValue.substring(strValue.indexOf("\t",strValue.indexOf("\t")+1)+1) ;
            voteValueOfNode = rank/(numOutlinks);
        }

        output.collect(new Text(pageTitle), new Text("#"+"\t"+outlinks));
        for (int i = 2 ; i < links.length; i++)
            output.collect(new Text(links[i]), new Text(Double.toString(voteValueOfNode)));
    }
}
