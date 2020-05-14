package leetcode;

class ListNode {
    int val;
    ListNode next;
    ListNode(int x) { val = x; }
}
public class leetcode_002_addTwoNumbers {
    public static void listNodePrint(ListNode listNode) {

        if (listNode == null) {
            return;
        }

        if (listNode.next == null) {
            System.out.println(listNode.val);
            return;
        }

        System.out.println(listNode.val);
        listNodePrint(listNode.next);
    }

    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        //定义满十进一的数
        int num = 0;
        //定义一个ListNode，作为链表头
        ListNode proNode = new ListNode(0);
        //定义一个ListNode，接受两数的和
        ListNode currentNode = new ListNode(0);
        //先连接两个Node
        proNode.next=currentNode;

        do {
            //两数相加
            int sum = (l1!=null?l1.val:0) + (l2!=null?l2.val:0) + num;
            //是否满十
            num = sum/10;
            //得出个位数
            int result = sum%10;
            //填入结果
            currentNode.val = result;
            l1 = l1!=null?l1.next:l1;
            l2 = l2!=null?l2.next:l2;
            if(l1!=null || l2!=null || num!=0) {
                currentNode.next = new ListNode(0);
                currentNode = currentNode.next;
            }
        }while(l1!=null || l2!=null || num!=0);
        return proNode.next;

    }
    public static void main(String[] args) {
// 链表 1
        ListNode list1Node1 = new ListNode(2);
        ListNode list1Node2 = new ListNode(4);
        ListNode list1Node3 = new ListNode(3);
        list1Node1.next = list1Node2;
        list1Node2.next = list1Node3;

        // 链表 2
        ListNode list2Node1 = new ListNode(5);
        ListNode list2Node2 = new ListNode(6);
        ListNode list2Node3 = new ListNode(4);
        list2Node1.next = list2Node2;
        list2Node2.next = list2Node3;

        ListNode resListNode = addTwoNumbers(list1Node1, list2Node1);

        listNodePrint(resListNode);

    }


}
