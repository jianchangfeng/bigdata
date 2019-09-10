package leetcode;

import java.util.ArrayList;
import java.util.List;

public class YHTriangle_119 {

    public List<Integer> getRow(int rowIndex){
        List<Integer> row = new ArrayList<>();
        for (int i =0; i <=rowIndex;i++ ) {
            row.add(0, 1);
            for (int j = 1; j < i; j++) {
                row.set(j, row.get(j) + row.get(j + 1));
            }
        }
        return row;
    }

    public static void main(String[] args){
        int numRow=3;
        List<Integer> al;
        YHTriangle_119  YH = new YHTriangle_119();
        al = YH.getRow(numRow);
        System.out.println(al);



    }

}
