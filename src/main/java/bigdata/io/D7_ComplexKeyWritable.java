package bigdata.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class D7_ComplexKeyWritable implements WritableComparable<D7_ComplexKeyWritable> {
    private long pv;
    private long click;

    public D7_ComplexKeyWritable(){}
    public D7_ComplexKeyWritable(long pv,long click){
        this.pv = pv;
        this.click = click;
    }

    @Override
    public int compareTo(D7_ComplexKeyWritable o) {
        //pv降序，如果pv相同，再按照click降序
        if(this.pv != o.pv){
            return this.pv > o.pv ? -1 : 1;
        }else {
            return this.click > o.click ? -1 : (this.click == o.click ? 0 : 1);
        }
    }
    /**
     * 用于序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.pv);
        dataOutput.writeLong(this.click);
    }
    /**
     * 用于反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pv = dataInput.readLong();
        this.click = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.pv + "\t" + this.click;
    }


    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public long getClick() {
        return click;
    }

    public void setClick(long click) {
        this.click = click;
    }
}
