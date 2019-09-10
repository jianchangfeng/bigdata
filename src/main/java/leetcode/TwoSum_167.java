package leetcode;

public class TwoSum_167 {
    public int[] addTwo(int[] numbers, int target){
        int i =0, j=numbers.length-1;
        while(i<j) {
            int numSum = numbers[i] + numbers[j];
            if (numSum < target) {
                i++;
            } else if (numSum > target) {
                j--;
            } else {
                return new int[]{i+1, j+1};
            }
        }
        return new int[]{1, 1};

    }

    public static void main(String[] args) {
        int[] nums = { 1, 3, 4, 6, 9, 10, 11, 15 };
        int target = 11;
        int[] ret;
        TwoSum_167 self = new TwoSum_167();
        ret = self.addTwo(nums,target);
        for (int i =0; i < ret.length; i++){
            System.out.println(ret[i]);
        }
    }
}
