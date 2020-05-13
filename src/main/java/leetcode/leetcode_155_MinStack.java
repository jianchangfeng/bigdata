package leetcode;

import java.util.Stack;

public class leetcode_155_MinStack {
    private Stack<Integer> stackData;
    private Stack<Integer> stackMin;
    /** 构造方法 */
    public leetcode_155_MinStack() {
        stackData = new Stack<Integer>();
        stackMin = new Stack<Integer>();
    }

    /*将元素x推入栈中*/
    public void push(int x) {
        if(stackMin.empty()) {
            stackMin.push(x);
        } else if(x <= getMin()) {
            stackMin.push(x);
        } else {
            int newMin = getMin();
            stackMin.push(newMin);
        }
        stackData.push(x);
    }

    /*删除栈顶元素*/
    public void pop() {
        if(stackData.empty()) {
            throw new RuntimeException("your stack is empty!");
        }
        stackMin.pop();
        stackData.pop();
    }

    /*获取栈顶元素*/
    public int top() {
        if(stackData.empty()) {
            throw new RuntimeException("your stack is empty!");
        }
        return stackData.peek();
    }

    /*检索栈中的最小元素*/
    public int getMin() {
        if(stackMin.empty()) {
            throw new RuntimeException("your stack is empty!");
        }
        return stackMin.peek();
    }

        public static void main(String[] args) {
            leetcode_155_MinStack minStack = new leetcode_155_MinStack();
            minStack.push(-2);
            minStack.push(0);
            minStack.push(-3);
            System.out.println(minStack.getMin());
            minStack.pop();
            minStack.top();
            System.out.println(minStack.getMin());
        }

}
