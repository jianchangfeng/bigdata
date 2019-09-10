package leetcode;

import java.util.HashMap;

public class majorityElement_169 {
    public int getmajorityElement(int[] nums){
        HashMap d = new HashMap();
        for (int num : nums){
            d.put(num,0);
        }
        for (int num : nums){
            d.put(num,(int)d.get(num)+1);
        }
        for (Object key : d.keySet()){
            if ((int)d.get(key) > nums.length/2){
                return (int)key;
            }
        }
        return -1;
    }
    public static void main(String[] args){
        int[] nums = { 2,2,1,1,1,2,2 };
        int majorityElement;
        majorityElement_169 self = new majorityElement_169();
        majorityElement = self.getmajorityElement(nums);
        System.out.println(majorityElement);
    }
}
