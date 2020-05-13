package leetcode;

import java.util.HashMap;

public class leetcode_219_containsDuplicate {
    public boolean checkcontainsDuplicate(int[] nums, int k){
        HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
        for (int i=0;i<nums.length;i++){
            if(map.containsKey(nums[i])){
                if(i-map.get(nums[i])<=k){
                    return true;
                }
            }
            map.put(nums[i],i);
        }
        return false;
    }
    public static void main(String[] args){
        int[] nums = {1,0,0,1};
        int k =1;
        leetcode_219_containsDuplicate self = new leetcode_219_containsDuplicate();
        System.out.println(self.checkcontainsDuplicate(nums,k));
    }
}
