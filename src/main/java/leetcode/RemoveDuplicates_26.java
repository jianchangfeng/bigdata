package leetcode;

public class RemoveDuplicates_26 {
    public int toRemoveDuplicates(int[] nums){
        int i=0;
        for (int j=0; j<nums.length;j++){
            if (nums[j] != nums[i]){
                nums[i+1] = nums[j];
                i++;
            }
        }
        return i+1;
    }
    public static void main(String[] args){
        int[] nums = {0,0,1,1,1,2,2,3,3,4};
        int output_lengh;
        RemoveDuplicates_26 ReDuNum = new RemoveDuplicates_26();
        output_lengh = ReDuNum.toRemoveDuplicates(nums);
        System.out.println(output_lengh);

    }


}
