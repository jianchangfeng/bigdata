package bigdata.HBase.MapReduce;

import bigdata.mapreduce.log_analysis.*;
import org.apache.hadoop.util.ProgramDriver;

public class Driver {
    public static void main(String[] args) throws Throwable {
        ProgramDriver pgd = new ProgramDriver();
        pgd.addClass("ImportFromHDFS", ImportFromHDFS.class, "Import from hdfs to HBase");
        pgd.addClass("HBaseToHDFS", HBaseToHDFS.class, "HBase to HDFS");
        pgd.addClass("HBaseToHDFSZuoYe", HBaseToHDFSZuoYe.class, "HBaseToHDFSZuoYe");
        pgd.addClass("D1_Get_detail_of_one_advertisement", D1_Get_detail_of_one_advertisement.class, "HBaseToHDFSZuoYe");
        pgd.addClass("D2_01_SumGroupByMRJobNew", D2_01_SumGroupByMRJobNew.class, "HBaseToHDFSZuoYe");
        pgd.addClass("D2_02_SumGroupByCombinerMRJobNew", D2_02_SumGroupByCombinerMRJobNew.class, "D2_02_SumGroupByCombinerMRJobNew");
        pgd.addClass("D3_OrderByDescMRJobNew", D3_OrderByDescMRJobNew.class, "D3_OrderByDescMRJobNew");
        pgd.addClass("D4_CaseWhenSumGroupByMRJobNew", D4_CaseWhenSumGroupByMRJobNew.class, "D4_CaseWhenSumGroupByMRJobNew");
        pgd.addClass("D5_ReduceSideJoinMRJobNew", D5_ReduceSideJoinMRJobNew.class, "D5_ReduceSideJoinMRJobNew");
        pgd.addClass("D6_MapSideJoinMRJobNew", D6_MapSideJoinMRJobNew.class, "D6_MapSideJoinMRJobNew");
        pgd.addClass("D7_SecondSortMRJobNew", D7_SecondSortMRJobNew.class, "D7_SecondSortMRJobNew");
        pgd.addClass("Test_Submit_SelectUVByAreaAndDate", Test_Submit_SelectUVByAreaAndDate.class, "Test_Submit_SelectUVByAreaAndDate");
        pgd.driver(args);

    }
}
