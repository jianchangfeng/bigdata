package leetcode;

public class leetcode_033_search {
    public int search(int[] nums, int target) {
        if (nums.length == 0) return -1;
        int right = nums.length - 1;
        int left = 0;
        while (left <= right) {
            int mid = 0 + (left + right) / 2;
            if (target == nums[left]) {
                return left;
            } else if (target == nums[right]) {
                return right;
            } else if (target == nums[mid]) {
                return mid;
            } else {
                if (target > nums[mid]) {
                    if (nums[mid] < nums[0]) {
                        if (target < nums[0]) {//3
                            left = mid + 1;
                        } else {//1
                            right = mid - 1;
                        }
                    } else {//1
                        left = mid + 1;
                    }
                } else if (target < nums[mid]) {
                    if (nums[mid] > nums[0]) {
                        if (target < nums[0]) {//5
                            left = mid + 1;
                        } else {//4
                            right = mid - 1;
                        }

                    } else {//6
                        right = mid - 1;
                    }
                }
            }

        }
        return -1;
    }
    public static void main(String[] args) {
        int[] nums = { 4,5,6,7,0,1,2 };
        int target = 2;
        int ret;
        leetcode_033_search self = new leetcode_033_search();
        ret = self.search(nums, target);
        System.out.println(ret);
    }

}
