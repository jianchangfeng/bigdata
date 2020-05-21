package leetcode;

import java.util.HashMap;

public class leetcode_560_subarraySum {
    public int subarraySum(int[] nums, int k) {
        int count = 0;
        HashMap<Integer, Integer> map = new HashMap<>();
        map.put(0, 1);
        int sum = 0;
        for(int i = 0; i < nums.length; i++){
            sum += nums[i];
            if(map.containsKey(sum - k))
                count += map.get(sum - k);
            if(map.containsKey(sum))// 已有和
                map.put(sum, map.get(sum) + 1);
            else// 新的和
                map.put(sum, 1);
        }
        return count;
    }
    public static void main(String[] args) {
        int[] nums = {1,1,1};
        int k = 2;
        int res;
        leetcode_560_subarraySum Sum_K = new leetcode_560_subarraySum();
        res = Sum_K.subarraySum(nums,k);
        System.out.println(res);
    }

}
