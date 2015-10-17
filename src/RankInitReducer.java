import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gokul on 10/13/15.
 *
 * This is Initialization Reducer which maps values to temp/Job4/PageRank.iter0.out
 * All the values are written in following format
 * <PageTitle>  <InitializedPageRank i.e. 1/numberOfPages>   <Outlinks from the current Page(tab seperated)>
 */
public class RankInitReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public String numberOfPagesStr;

    public void configure(JobConf jobConf){
        numberOfPagesStr = jobConf.get("numberOfPages");
    }
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double numberOfPages = 1.0 / Long.parseLong(numberOfPagesStr);
        String strValues = "";

        while(values.hasNext()){
            strValues += values.next().toString();
        }

        output.collect(key, new Text(Double.toString(numberOfPages)+"\t"+strValues));
    }
}
