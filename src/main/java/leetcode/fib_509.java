package leetcode;

public class fib_509 {
    public int getFib(int N){
        if (N == 0 || N==1){
            return N;
        }
        return getFib(N - 1) + getFib(N - 2);

    }

    public static void main(String[] args) {
        int n=4;
        int res;
        fib_509 fibnum = new fib_509();
        res=fibnum.getFib(n);
        System.out.println(res);
    }

}
