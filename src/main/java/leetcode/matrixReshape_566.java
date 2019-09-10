package leetcode;

public class matrixReshape_566 {
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
       matrixReshape_566 natRes = new matrixReshape_566();
       res=natRes.tomatrixReshape(nums,r,c);
       System.out.println(res);

    }
}
