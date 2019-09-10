package leetcode;

import java.util.HashMap;

public class containsDuplicate_219 {
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
        containsDuplicate_219 self = new containsDuplicate_219();
        System.out.println(self.checkcontainsDuplicate(nums,k));
    }
}
