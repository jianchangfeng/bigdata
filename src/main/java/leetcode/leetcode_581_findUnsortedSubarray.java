package leetcode;

import java.util.Arrays;

public class leetcode_581_findUnsortedSubarray {
    public int tofindUnsortedSubarray(int[] nums){
        int[] sorted_nums=nums.clone();
        Arrays.sort(sorted_nums);
        int p1=0,p2=nums.length-1;
        while (p1<=p2&&sorted_nums[p1]==nums[p1]){ p1+=1;}
        while (p1<=p2&&sorted_nums[p2]==nums[p2]) { p2-=1;}
        return p2-p1+1;
    }
    public static void main(String[] args) {
        int[] nums={2, 6, 4, 8, 10, 9, 15};
        int res_length;
        leetcode_581_findUnsortedSubarray findUS = new leetcode_581_findUnsortedSubarray();
        res_length=findUS.tofindUnsortedSubarray(nums);
        System.out.println(res_length);

    }
}
