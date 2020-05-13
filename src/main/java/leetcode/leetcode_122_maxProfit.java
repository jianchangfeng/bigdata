package leetcode;

public class leetcode_122_maxProfit {
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
        leetcode_122_maxProfit max_Pro = new leetcode_122_maxProfit();
        maxprofit = max_Pro.getmaxProfit(prices);
        System.out.println(maxprofit);
    }

}
