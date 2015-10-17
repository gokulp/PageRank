import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * Created by gokul on 10/12/15.
 */
public class RedLinkReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> set = new HashSet();

        while (values.hasNext()) {
            set.add(values.next().toString());
        }

        String adjLink = null;
        if (set.contains("!")) {
            //output.collect(key, new Text("Link")); //This is Link
            Iterator it = set.iterator();
            while (it.hasNext()) {
                adjLink = it.next().toString();
                if (!adjLink.equals("!")) {
                    output.collect(new Text(adjLink), key);
                } else {
                    output.collect(key , new Text("!"));
                }
            }
        }


        //Commented for Job2
        /*else {
            output.collect(key, new Text("RedLink"));
        }*/
    }
}
