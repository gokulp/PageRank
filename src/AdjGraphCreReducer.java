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
public class AdjGraphCreReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder adjList = new StringBuilder();
        if (key.toString().equals("!")){
            //There are no target links from this string
            while (values.hasNext()){
                output.collect(new Text(values.next()), new Text(""));
            }
        } else {
            while (values.hasNext()){
                String link = values.next().toString();
                if (!link.equals("!"))
                    adjList.append(link + "\t");
            }
            if (adjList.length() > 0)
                adjList.deleteCharAt(adjList.length() - 1); //Delete extra tab at the end
            output.collect(key, new Text(adjList.toString()));
        }
    }
}
