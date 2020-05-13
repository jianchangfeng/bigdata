package leetcode;

public class leetcode_643_findMaxAverage {
    public double tofindMaxAverage(int[] nums, int k){
        int ans = 0, sum =0;
        for (int i =0; i<k; i++){
            sum += nums[i];
        }
        ans = sum;
        for (int i =k; i< nums.length;i++){
            sum = sum - nums[i-k] + nums[i];
            ans = Math.max(ans, sum);
        }
        return ((double)ans)/k;
    }

    public static void main(String[] Args){
        int nums[] = {1,12,-5,-6,50,3};
        int k = 4;
        double res;
        leetcode_643_findMaxAverage FMA = new leetcode_643_findMaxAverage();
        res=FMA.tofindMaxAverage(nums,k);
        System.out.println(res);
    }
}
