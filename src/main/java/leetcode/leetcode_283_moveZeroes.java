package leetcode;

public class leetcode_283_moveZeroes {
    public int[] tomoveZeroes(int[] nums){
        int i=0,j=0;
        for (i=0;i<nums.length;i++){
            if (nums[i] != 0){
                nums[j++]=nums[i];
            }
        }
        while (j < nums.length){
            nums[j++] = 0;
        }
        return nums;
    }

    public static void main(String[] args) {
        int[] nums = {0,1,0,3,12};
        int[] output;
        leetcode_283_moveZeroes ReZoeo = new leetcode_283_moveZeroes();
        output = ReZoeo.tomoveZeroes(nums);
        for (int i=0;i<output.length;i++){
            System.out.println(output[i]);
        }

    }
}
