package leetcode;

public class RotateArray_189 {
    public int[] toRotate(int[] nums, int key){
        Recursive(nums,key,0,nums.length);
        return nums;
        }

    private void Recursive(int[] nums, int key, int start,  int length) {
        key %= length;
        System.out.println(key);
        System.out.println(start);
        System.out.println(length);
        if (key != 0){
            for (int i = 0;i < key; i++) {
                toswap(nums, start + i, nums.length - key + i);
            }
//            Recursive(nums,key,start+key,length - key);
        }
    }

    private void toswap(int[] nums, int i, int j) {
        nums[i] = nums[i] + nums[j];
        nums[j] = nums[i] - nums[j];
        nums[i] = nums[i] - nums[j];
    }

    public static void main(String[] args){
        int[] nums = {1,2,3,4,5,6,7};
        int[] output;
        int key = 3;
        RotateArray_189 self = new RotateArray_189();
        output = self.toRotate(nums,key);
        for (int i = 0; i<nums.length;i++){
            System.out.println(output[i]);
        }
    }
}
