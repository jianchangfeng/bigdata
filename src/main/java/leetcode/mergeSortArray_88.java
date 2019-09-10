package leetcode;

public class mergeSortArray_88 {
    public void  getmergeSortArray(int[] nums1, int m, int[] nums2, int n){
        int i=m-1,j=n-1,k=m+n-1;
        while (i>=0&&j>=0){
            nums1[k--] = (nums1[i] >=nums2[j] ? nums1[i--] : nums2[j--]);
            }
        if (j>=0){
            System.arraycopy(nums2,0,nums1,0,j+1);
        }
    }
    public static void main(String[] args) {
        int[] nums1={1,2,3,0,0,0};
        int m=3;
        int[] nums2={2,5,6};
        int n=3;
        mergeSortArray_88 merSortArray = new mergeSortArray_88();
        merSortArray.getmergeSortArray(nums1,m,nums2,n);
        for (int i =0;i<nums1.length;i++){
            System.out.println(nums1[i]);
        }
    }
}
