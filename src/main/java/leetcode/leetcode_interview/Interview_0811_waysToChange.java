package leetcode.leetcode_interview;

public class Interview_0811_waysToChange {
    public int waysToChange(int n) {
        int mod = 1000000007;
        int[] type = {1, 5, 10, 25};
        int[] dp = new int[n + 1];
        dp[0] = 1;
        for (int i = 0; i < type.length; i++) {
            for (int j = type[i]; j <= n; j++) {
                dp[j] = dp[j] + dp[j - type[i]];
                if (dp[j] >= mod) {
                    dp[j] = dp[j] % mod;
                }
            }
        }
        return dp[n];

    }

    public static void main(String[] args) {
        int num = 10;
        int ret;
        Interview_0811_waysToChange self = new Interview_0811_waysToChange();
        ret = self.waysToChange(num);
        System.out.println(ret);
        }

}

