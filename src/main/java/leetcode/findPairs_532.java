package leetcode;

import java.util.HashMap;

public class findPairs_532 {
    public int tofindPairs(int[] nums,int k){
        if (k<0) return 0;
        int res = 0;
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int num:nums){
            map.put(num, map.containsKey(num)? 1+map.get(num):1);
        }
        if (k==0){
            for (int num:map.keySet()){
                if(map.get(num) > 1)
                    res++;
            }
        }else {
            for (int num:map.keySet()){
                if(map.containsKey(num+k))
                    res++;
            }
        }return res;
    }

    public static void main(String[] args){
        int[] nums={3, 1, 4, 1, 5};
        int k=2;
        int res;
        findPairs_532 FP = new findPairs_532();
        res=FP.tofindPairs(nums,k);
        System.out.println(res);
    }
}
