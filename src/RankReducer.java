import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gokul on 10/13/15.
 * This routine calculates PageRank of each page by collecting output from mapper.
 * Every respective page mapper will send outlinks from the page.
 * Every other page pointing to this page will send their respective vote value.
 *
 * As all the RedLinks are removed in First Job here we don't consider adding conditions for redlinks.
 */
public class RankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public static String numberOfPagesStr;

    public void configure(JobConf jobConf){
        numberOfPagesStr = jobConf.get("numberOfPages");
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double numberOfPages = Double.parseDouble(numberOfPagesStr);
        double rank = 0.0;
        double votes = 0.0;
        double d= 0.85;
        String outlinks = "";
        String pageTitle = key.toString();

        while (values.hasNext()){
            String strValue = values.next().toString();
            String[] partValue = strValue.split("\t");
            if ( partValue[0].equals("#") ) {
                //Then this is the string which was formed from adjacency graph for outlink count

                //Add all outlinks to outlinks string
                for (int i = 1; i < partValue.length; i++) {
                    outlinks += "\t"+partValue[i];
                }
            } else {
                //This is incoming vote sent by other nodes
                votes += Double.parseDouble(strValue);
            }
        }

        rank = (0.15 / numberOfPages) + (0.85 * votes);
        output.collect(new Text(pageTitle), new Text(Double.toString(rank)+outlinks));
    }
}
