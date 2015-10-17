import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by gokul on 10/13/15.
 */
public class MyKeyComparison extends WritableComparator {
    protected MyKeyComparison() {
        super(DoubleWritable.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable d1 = (DoubleWritable) w1;
        DoubleWritable d2 = (DoubleWritable) w2;
        int cmp = d1.compareTo(d2);
        return cmp * -1; //reverse
    }
}
