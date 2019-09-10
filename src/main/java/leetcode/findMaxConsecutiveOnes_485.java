package leetcode;

public class findMaxConsecutiveOnes_485 {
    public int tofindMaxConsecutiveOnes(int[] nums){
        int max = 0;
        int count = 0;
        for (int i =0;i<nums.length;i++){
            System.out.println("count is" + ' ' +  count);
            System.out.println("max is" + ' ' + max);
            if (nums[i]==1){
                count++;
                max=count>max?count:max;
            }else {
                count=0;
            }
        }
        return max;
    }
    public static void main(String[] args){
        int[] nums = {1,1,0,1,1,1,0,0,0,1,1,0,0,0,0};
        int res;
        findMaxConsecutiveOnes_485 findMaxC = new findMaxConsecutiveOnes_485();
        res=findMaxC.tofindMaxConsecutiveOnes(nums);
        System.out.println("############last###############");
        System.out.println(res);
    }
}
