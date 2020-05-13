package leetcode;

public class leetcode_509_fib {
    public int getFib(int N){
        if (N == 0 || N==1){
            return N;
        }
        return getFib(N - 1) + getFib(N - 2);

    }

    public static void main(String[] args) {
        int n=4;
        int res;
        leetcode_509_fib fibnum = new leetcode_509_fib();
        res=fibnum.getFib(n);
        System.out.println(res);
    }

}
