package leetcode;

import java.util.HashMap;
import java.util.Map;

public class leetcode_001_SumTwoNum {
    public static int[] addTwoNum(int[] nums,int target){
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++){
            if(map.containsKey(target - nums[i]))
                return new int[] {map.get(target - nums[i]), i};
            map.put(nums[i], i);
        }
        return new int[] {-1, -1};
    }

    public static void main(String[] args) {
        int[] nums = { 1, 3, 20, 30, 54, 8, 9, 10 };
        int target = 31;
        int[] ret;
        leetcode_001_SumTwoNum self = new leetcode_001_SumTwoNum();
        ret = self.addTwoNum(nums, target);
        for (int i =0; i < ret.length; i++){
            System.out.println(ret[i]);
        }
    }

}
