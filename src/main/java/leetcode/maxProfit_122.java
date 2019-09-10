package leetcode;

public class maxProfit_122 {
    public int getmaxProfit(int[] prices){
        int Re_profit = 0;
        for (int i = 0; i<(prices.length-1); i++){
            int d = prices[i+1] - prices[i];
            if (d>0){
                Re_profit += d;
            }
        }
        return Re_profit;
    }
    public static void main(String[] args) {
        int[] prices = { 1, 3, 20, 30, 54, 8, 9, 10};
        int maxprofit = 0;
        maxProfit_122 max_Pro = new maxProfit_122();
        maxprofit = max_Pro.getmaxProfit(prices);
        System.out.println(maxprofit);
    }

}
