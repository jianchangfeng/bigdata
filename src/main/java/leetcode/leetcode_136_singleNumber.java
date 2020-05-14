package leetcode;

public class leetcode_136_singleNumber {
    public int singleNumber(int[] nums) {
        int ans = nums[0];
        for (int i = 1, len = nums.length; i < len; i++)
            ans ^= nums[i];
        return ans;
    }

    public static void main(String[] args) {
        int[] nums = {985, 211, 1, 1, 2, 985, 211, 998, 998};
        leetcode_136_singleNumber slef = new leetcode_136_singleNumber();
        System.out.println("数组中只出现一次的元素是:" + slef.singleNumber(nums));
    }
}
