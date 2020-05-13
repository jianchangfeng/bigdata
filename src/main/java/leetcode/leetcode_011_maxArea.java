package leetcode;

public class leetcode_011_maxArea {
    public int maxArea(int[] height) {
        int max = 0;
        int i = 0;
        int j = height.length - 1;
        while (i < j) {
            int width = j - i;
            // 计算短边的最大矩形面积
            int area = width * Math.min(height[i], height[j]);
            max = Math.max(area, max);
            // 抛弃短边
            if (height[i] < height[j]) {
                i++;
            } else {
                j--;
            }
        }

        return max;
    }

    public static void main (String[]args){
            int[] nums = {1, 8, 6, 2, 5, 4, 8, 3, 7};
            int ret;
            leetcode_011_maxArea self = new leetcode_011_maxArea();
            ret = self.maxArea(nums);
            System.out.println(ret);
        }
    }

