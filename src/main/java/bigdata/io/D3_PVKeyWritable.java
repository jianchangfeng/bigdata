package bigdata.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class D3_PVKeyWritable implements WritableComparable<D3_PVKeyWritable> {
    private long pv;


    public D3_PVKeyWritable(){}

    @Override
    public int compareTo(D3_PVKeyWritable o) {
        //升序：当 a > b,返回一个正数，一般是1
        //降序：当 a > b,返回一个负数，一般是-1
        return this.pv > o.pv ? -1 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.pv);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pv = dataInput.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(this.pv);
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }
}
