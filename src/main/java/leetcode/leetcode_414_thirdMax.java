package leetcode;
//解题思路：
//        遍历数组，可以借鉴求最大值的思路，只不过扩展一下，
//        求解前三大的数字而已，利用判断语句判断nums当前的值的范围，
//        如果比最大值大就替代，在最大值第二大值之间就替代第二大值，
//        在第二大值及第三大值之间就替代第三大值，否则跳过。
//        有一点需要注意的是赋值的顺序，不能有交叉影响下一个复制表达式。
public class leetcode_414_thirdMax {
    public int getthirdMax(int[] nums){
        int max1=Integer.MIN_VALUE;
        int max2=Integer.MIN_VALUE;
        int max3=Integer.MIN_VALUE;
        int k =0;
        boolean j=true;
        for (int i=0;i<nums.length;i++){
            if (nums[i] == Integer.MIN_VALUE&&j){
                k++;
                j=false;
            }
            if (nums[i]>max1){
                k++;max3=max2;max2=max1;max1=nums[i];
            }else if (nums[i] > max2 && nums[i]<max1){
                k++;max3=max2;max2=nums[i];
            }else if (nums[i] > max3 && nums[i]<max2){
                k++;max3=nums[i];
            }
        }
        return k>=3?max3:max1;
    }

    public static void main(String[] args){
        int[] nums = {2,2,3,1};
        int resout;
        leetcode_414_thirdMax ThirNum = new leetcode_414_thirdMax();
        resout=ThirNum.getthirdMax(nums);
        System.out.println(resout);
    }
}
