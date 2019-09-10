package bigdata.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Test_tmp_ByAreaDate_Writable implements Writable {
    private String date;
    private String areaId;
    private long pv;
    private long click;

    //反序列化时，需要调用空参构造方法，如果空参构造方法被覆盖，一定要显示定义一个空参构造方法
    public Test_tmp_ByAreaDate_Writable(){}

    public Test_tmp_ByAreaDate_Writable(String date, String areaId, long pv, long click){
        this.date = date;
        this.areaId = areaId;
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
        dataOutput.writeUTF(this.date);
        dataOutput.writeUTF(this.areaId);
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
        this.date = dataInput.readUTF();
        this.areaId = dataInput.readUTF();
        this.pv = dataInput.readLong();
        this.click = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.date + "\t" + this.areaId + "\t" + this.pv + "\t" + this.click;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getAreaId() {
        return areaId;
    }

    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }

    public void plusPv(long pv){
        this.pv += pv;
    }

    public void plusClick(long click){
        this.click += click;
    }

}
