package leetcode;
//思路提示：
//        解决本题之前，先复习一下二分查找法：
//        给定一个排序数组，在数组中查找目标值，并返回索引。
//        这里特别需要注意的地方就是 边界 的设置，也就是 r 设置成 n 还是 n - 1，
//        循环条件设置成 l < r 还是 l <= r，其实无论 边界 设置为哪种情况，
//        都可以写出正确的二分查找程序，关键是要提前对 边界 进行清晰地定义，
//        之后书写代码逻辑的时候严格遵循这个定义即可。

public class searchInsert_35 {
    public int binSearch(int[] nums, int target){
        int l =0;
        int r = nums.length;
        while (l < r){
            int mid = (l+r)/2;
            if (nums[mid] == target)
                return mid;
            else  if (nums[mid] > target)
                r=mid;
            else
                l=mid+1;
        }
        return l;
    }

    public static void main(String[] args){
        int[] nums= {1,3,5,6};
        int serNum = 8;
        int res;
        searchInsert_35 SerInser = new searchInsert_35();
        res = SerInser.binSearch(nums,serNum);
        System.out.println(res);
    }
}
