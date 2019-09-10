package bigdata.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class D4_TerminalTypeReportWritable implements Writable {
    private String filename = "";
    private String terminalType = "";
    private long pv;
    private long click;

    //反序列化时，需要调用空参构造方法，如果空参构造方法被覆盖，一定要显示定义一个空参构造方法
    public D4_TerminalTypeReportWritable(){}

    public D4_TerminalTypeReportWritable(String terminalType, long pv, long click){
        this.terminalType = terminalType;
        this.pv = pv;
        this.click = click;
    }

    /**
     * 用于序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.terminalType);
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
        //反序列化的顺序要与序列化的顺序一致
        this.terminalType = dataInput.readUTF();
        this.pv = dataInput.readLong();
        this.click = dataInput.readLong();

    }

    @Override
    public String toString() {
        return this.terminalType + "\t" + this.pv + "\t" + this.click;
    }

    public void plusPv(long pv){
        this.pv += pv;
    }

    public void plusClick(long click){
        this.click += click;
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

    public String getTerminalType() {
        return terminalType;
    }

    public void setTerminalType(String terminalType) {
        this.terminalType = terminalType;
    }

    public String getfilename() {
        return filename;
    }

    public void setfilename(String filename) {
        this.filename = filename;
    }
}
