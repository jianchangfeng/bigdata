package leetcode;

public class plusOne_66 {
    public int[] toplusOne(int[] nums){
        int i = nums.length-1;
        while (true) {
            if (i<0){
                int[] newArr = new int[nums.length+1];
                System.arraycopy(nums,0,newArr,1,nums.length);
                nums=newArr;i=0;
            }
            if (nums[i]+1<10){
                nums[i] = nums[i] + 1;
                break;
            }
            nums[i] = 0;
            i--;
        }
        return nums;

    }

    public static void main(String[] args) {
        int[] nums = {4,3,2,1};
        int[] output;
        plusOne_66 PO = new plusOne_66();
        output = PO.toplusOne(nums);
        for (int i=0;i<output.length;i++){
            System.out.println(output[i]);
        }

    }
}
