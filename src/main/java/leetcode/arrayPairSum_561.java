package leetcode;

import java.util.Arrays;

public class arrayPairSum_561 {
    public int arrayPairSum(int[] nums){
        Arrays.sort(nums);
        int sum = 0;
        for(int i=0;i<nums.length;i=i+2){
            sum+=nums[i];
        }
        return sum;
    }

    public static void main(String[] args) {
        int[] nums={1,4,3,2};
        int res;
        arrayPairSum_561 PairArray = new arrayPairSum_561();
        res = PairArray.arrayPairSum(nums);
        System.out.println(res);

    }

}
