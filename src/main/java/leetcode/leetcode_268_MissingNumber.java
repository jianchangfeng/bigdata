package leetcode;

public class leetcode_268_MissingNumber {
    public int getMissingNumber(int[] nums){
        int sum = 0;
        for (int i =0;i<nums.length;i++){
            sum += nums[i];
        }
        return (nums.length*(nums.length+1))/2 - sum;
    }
    public static void main(String[] args){
        int[] nums = {9,6,4,2,3,5,7,0,1};
        int output;
        leetcode_268_MissingNumber MissNum = new leetcode_268_MissingNumber();
        output = MissNum.getMissingNumber(nums);
        System.out.println(output);
    }
}
