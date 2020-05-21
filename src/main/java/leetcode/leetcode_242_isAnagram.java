package leetcode;

import java.util.Arrays;

public class leetcode_242_isAnagram {
    public boolean isAnagram(String s, String t) {
        if (s.length() != t.length()) {
            return false;
        }

        char[] str1 = s.toCharArray();
        char[] str2 = t.toCharArray();
        Arrays.sort(str1);
        Arrays.sort(str2);
        return Arrays.equals(str1, str2);
    }
    public static void main(String[] args) {
        String s = "anagram";
        String t = "nagaram";
        leetcode_242_isAnagram IS = new leetcode_242_isAnagram();
        System.out.println(IS.isAnagram(s,t));


    }
}
