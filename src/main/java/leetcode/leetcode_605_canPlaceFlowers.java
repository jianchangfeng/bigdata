package leetcode;

public class leetcode_605_canPlaceFlowers {
    public boolean checkcanPlaceFlowers(int[] flowerbed, int n) {
        int s=0,z=1;
        for (int i=0;i<flowerbed.length;i++){
            if (flowerbed[i]==1){s+=(z-1)/2;z=0;}
            else{++z;}
            if (s>=n) return true;
        }
        s+=z/2;
        return s>=n;

    }

    public static void main(String[] args) {
         int[] flowerbed={1,0,0,0,1};
         int n =1;
         boolean YON;
         leetcode_605_canPlaceFlowers canPF = new leetcode_605_canPlaceFlowers();
         YON=canPF.checkcanPlaceFlowers(flowerbed,n);
         System.out.println(YON);
    }
}
