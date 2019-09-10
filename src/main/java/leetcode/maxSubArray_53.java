package leetcode;
//解题思路：
//        利用动态规划思想，我们假设f(n)为末尾下标为n的最优子序列的和，
//        那么f(n-1)即为末尾下标为n-1的最优子序列和，A(n)为nums数组中下标为n的元素，
//        我们来考察三者的关系，即为：f(n)=max( f(n-1)+A(n), A(n) )，解释一下，
//        因为我们之前已经给过定义f(n)是以n为末尾的子序列，那么他只可能有两种情况，
//        要么是f(n-1)所在的子序列加上A(n)作为f(n)，要么就是只有A(n)这一个元素(因为A(n)时一定要存在的)，
//        所以我们只需要判断一下，哪种序列和最大，就保留这个最大值，作为以n为末尾的子序列的最大值。
//        这样我们遍历整个数组，把每个结果存起来，最后比较出最大值，
//        即为整体子序列的最大值(其下标所在位置即为最优子序列末尾下表位置)。

public class maxSubArray_53 {
    public int getMaxSubArray(int[] nums){
//        int[] dp = new int[nums.length];
//        dp[0] = nums[0];
//        int max = nums[0];
//        for (int i =1;i<nums.length;i++) {
//            dp[i] = nums[i] + (dp[i-1] > 0 ?  dp[i-1] : 0);
//            if (dp[i] > max)
//                max = dp[i];
//            }
//        return max;
            int result = Integer.MIN_VALUE;
            int max=0;
            for (int i=0;i<nums.length;i++){
                max = Math.max(max+nums[i], nums[i]);
                result = Math.max(result,max);
            }
            return result;
        }
    public static void  main(String[] args){
        int[] nums = {1};
        int output;
        maxSubArray_53 MaxSub = new maxSubArray_53();
        output = MaxSub.getMaxSubArray(nums);
        System.out.println(output);

    }
}
