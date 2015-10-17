import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Created by gokul on 10/13/15.
 */
public class MyPartitioner implements Partitioner<DoubleWritable, Text>{
    @Override
    public void configure(JobConf job) {}

    @Override
    public int getPartition(DoubleWritable key, Text value, int numPartitions) {
        double d = (Double.parseDouble(key.toString()));
        int n =(int) d * 100;
        return (int)(n / numPartitions) ;
    }
}
