package leetcode;

import java.util.ArrayList;
import java.util.List;

public class leetcode_448_findDisappearedNumbers {
    public List<Integer> tofindDisappearedNumbers(int[] nums) {
        for (int i : nums){
            nums[Math.abs(i)-1] = -Math.abs(nums[Math.abs(i)-1]);
        }
        List<Integer> res = new ArrayList<>();
        for (int i=0;i<nums.length;i++){
            if (nums[i]>0){
                res.add(i+1);
            }
        }
        return res;
    }
    public static void main(String[] args){
        int[] nums={4,3,2,7,8,2,3,1};
        List<Integer> output = new ArrayList<>();
        leetcode_448_findDisappearedNumbers findMiss = new leetcode_448_findDisappearedNumbers();
        output = findMiss.tofindDisappearedNumbers(nums);
        System.out.println(output);

    }
}
