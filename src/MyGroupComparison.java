import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by gokul on 10/13/15.
 */
public class MyGroupComparison extends WritableComparator {
    protected MyGroupComparison() {
        super(DoubleWritable.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable ip1 = (DoubleWritable) w1;
        DoubleWritable ip2 = (DoubleWritable) w2;
        return ip1.compareTo(ip2);
    }
}
