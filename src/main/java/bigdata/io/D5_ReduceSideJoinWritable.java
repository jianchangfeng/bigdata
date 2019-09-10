package bigdata.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class D5_ReduceSideJoinWritable implements Writable {
    private String date;
    private String areaId;
    private long pv;
    private long click;
    private long userCount;

    private String flag;//1：用户数文件，2：曝光、点击量文件

    //反序列化时，需要调用空参构造方法，如果空参构造方法被覆盖，一定要显示定义一个空参构造方法
    public D5_ReduceSideJoinWritable(String date, String areaId, long pv, long click){}

    public D5_ReduceSideJoinWritable(String date, String areaId, long pv, long click, long userCount, String flag){
        this.date = date;
        this.areaId = areaId;
        this.pv = pv;
        this.click = click;
        this.userCount = userCount;
        this.flag = flag;
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
        dataOutput.writeLong(this.userCount);
        dataOutput.writeUTF(this.flag);
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
        this.userCount = dataInput.readLong();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.date + "\t" + this.areaId + "\t" + this.pv + "\t" + this.click + "\t" + this.userCount;
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

    public long getUserCount() {
        return userCount;
    }

    public void setUserCount(long userCount) {
        this.userCount = userCount;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
