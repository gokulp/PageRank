import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gokul on 10/12/15.
 */
public class RedLinkMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final Pattern pattern = Pattern.compile("\\[\\[([^:\\]]*?)\\]\\]");
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String page = value.toString();
        String title = page.substring(page.indexOf("<title>")+7, page.indexOf("</title>")).replace(" ", "_");
        String text = page.substring(page.indexOf("<text"), page.indexOf("</text>"));
        Matcher match = pattern.matcher(text);
        output.collect(new Text(title), new Text("!"));
        while (match.find()) {
            String link = match.group(1);
            if (link != null && ! link.isEmpty() && !link.endsWith(".jpg") && !link.endsWith(".gif") && !link.endsWith(".png") &&
                    !link.endsWith(" ") && !link.contains("Help:") && !link.startsWith("../") && !link.startsWith(".")){
                if (link.contains("|")) {
                    String outlink = link.substring(0, link.indexOf("|")).replace(" ", "_");
                    if (!outlink.equals(title) && !link.endsWith(".jpg") && !link.endsWith(".gif")
                            && !link.endsWith(".png") && !link.endsWith(" ")) {
                        output.collect(new Text(outlink), new Text(title));
                    }
                } else {
                    String outlink = link.replace(" ", "_");
                    output.collect(new Text(outlink),new Text(title));
                }
            }
        }
    }
}
