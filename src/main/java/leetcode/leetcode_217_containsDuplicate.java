package leetcode;

import java.util.Arrays;

public class leetcode_217_containsDuplicate {
    public boolean checkcontainsDuplicate(int[] nums){
        Arrays.sort(nums);
        for(int i = 0; i < nums.length -1; i++){
            if(nums[i] == nums[i+1]){
                return true;
            }
        }
        return false;
    }
    public static void main(String[] args){
        int[] nums= {1,2,3,4,5,6,7,8};
        leetcode_217_containsDuplicate self = new leetcode_217_containsDuplicate();
        System.out.println(self.checkcontainsDuplicate(nums));
    }
}
