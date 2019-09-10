package leetcode;

public class longestCommonPrefix_14 {
    public String longestCommonPrefix(String[] strs){
        StringBuilder commonPrefix = new StringBuilder("");
        if (strs.length==1) return strs[0];
        else if (strs.length==0) return "";
        int minLength=strs[0].length();
        for (int i=0;i<strs.length;i++){
            if (strs[i].length()<minLength)
                minLength=strs[i].length();
            }

        for (int j=0;j<minLength;j++){
            for (int k=0;k<strs.length;k++){
                if (strs[k].charAt(j) != strs[0].charAt(j))
                    return commonPrefix.toString();
            }
            commonPrefix.append(strs[0].charAt(j));
        }
        return commonPrefix.toString();

    }
    public static void main(String[] args){
        String[] strs = {"flower","flow","flight"};
        String res_str;
        longestCommonPrefix_14 LCP = new longestCommonPrefix_14();
        res_str=LCP.longestCommonPrefix(strs);
        System.out.println(res_str);



    }
}
