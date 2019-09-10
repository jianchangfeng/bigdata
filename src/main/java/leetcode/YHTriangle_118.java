package leetcode;

import java.util.ArrayList;
import java.util.List;

public class YHTriangle_118 {
//    public int[][] generate(int numRows){
////        int[][] arr = new int[numRows][];
////        for (int i=0;i<numRows;i++){arr[i] = new int[i+1];}
////        arr[0][0] = 1;
////        for (int i=1;i<numRows;i++){
////            arr[i][0] = 1;
////            for (int j =1;j<i;j++){arr[i][j]=arr[i-1][j-1]+arr[i-1][j];}
////            arr[i][i]=1;
////        }
////        return arr;
////    }

    public List<List<Integer>> generate(int numRows){
        List<List<Integer>> arr = new ArrayList<>();
        if (numRows==0) return arr;
        arr.add(new ArrayList<Integer>());
        arr.get(0).add(1);
        for (int i =1;i<numRows;i++) {
            List<Integer> row = new ArrayList<>();
            row.add(1);
            for (int j = 1;j<i;j++) {
                row.add(arr.get(i - 1).get(j - 1)+ arr.get(i - 1).get(j));
            }
            row.add(1);
            arr.add(row);
        }
        return arr;
    }

    public static void main(String[] args){
        int numRows = 3;
//        int[][] res_YH;
        List<List<Integer>> al;
        YHTriangle_118 YH = new YHTriangle_118();
        al=YH.generate(numRows);
//        for (int i=0;i<res_YH.length;i++){
//            for (int j=0;j<res_YH[0].length;j++){
//                System.out.print(res_YH[i][j]);
//            }System.out.println();
        System.out.println(al);
        }
    }
