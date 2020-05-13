package leetcode;

public class leetcode_566_matrixReshape {
    public int[][] tomatrixReshape(int[][] nums,int r, int c){
        int row = nums.length, col = nums[0].length;
        if (row*col != r*c) return nums;
        int [][] res = new int[r][c];
        int index = 0;
        for (int i =0;i<r;i++){
            for (int j =0;j<c;j++){
                res[i][j] = nums[index/col][index%col];
                index++;
            }
        }return res;
    }
    public static void main(String[] args) {
       int[][] nums = {{1,2},{3,4}};
       int r =1;int c=4;
       int res[][];
       leetcode_566_matrixReshape natRes = new leetcode_566_matrixReshape();
       res=natRes.tomatrixReshape(nums,r,c);
       System.out.println(res);

    }
}
