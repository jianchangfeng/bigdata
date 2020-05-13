package leetcode;
import leetcode.leetcode_api.TreeNode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class leetcode_199_rightSideView {
    public List<Integer> rightSideView(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        rightSideView(root, 0, res);
        return res;
    }
    private void rightSideView(TreeNode node, int depth, List<Integer> res) {
        if(node == null) return;
        if(depth == res.size()) {
            res.add(node.val);
        }
        rightSideView(node.right, depth + 1, res);
        rightSideView(node.left, depth + 1, res);
    }
    public static void main(String[] args) {
        String str = "[1,2,3,null,5,null,4]";
        TreeNode node = TreeNode.mkTree(str);
        leetcode_199_rightSideView self = new leetcode_199_rightSideView();
        System.out.println(self.rightSideView(node));
    }
}

