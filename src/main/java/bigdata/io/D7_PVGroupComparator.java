package bigdata.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class D7_PVGroupComparator extends WritableComparator {
    public D7_PVGroupComparator(){
        super(D7_ComplexKeyWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        D7_ComplexKeyWritable key1 = (D7_ComplexKeyWritable) a;
        D7_ComplexKeyWritable key2 = (D7_ComplexKeyWritable) b;
        return key1.getPv() == key2.getPv() ? 0 : 1;
    }
}
