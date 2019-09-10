package Java_base;

import java.util.*;

public class JavaTest_set {
    public static void main(String[] args){
        System.out.println("###########ArrayList Demon Start###############################");
        ArrayList List1 = new ArrayList();
        System.out.println("Initial Size of list1 is :" + ' ' + List1.size());
        //Add element to the array list
        List1.add("C");
        List1.add("A");
        List1.add("E");
        List1.add("B");
        List1.add("D");
        List1.add("F");
        List1.add(1,"A");
        System.out.println("Size of List1 after additiion:" + ' ' + List1.size());

        //Display the arrary list
        System.out.println("Content of List1:" +  ' ' +  List1);
        //Remove Element
        List1.remove("F");
        List1.remove(2);
        System.out.println("Size of List1 after deletions:" + ' ' + List1.size());
        System.out.println("Content of List1:" +  ' ' +  List1);
        System.out.println("###########ArrayList Demon end ###############################");
        System.out.println();
        System.out.println("###########LinkedList Algorithms Demon Start###############################");
        LinkedList link1 = new LinkedList();
        link1.add(new Integer(-8));
        link1.add(new Integer(20));
        link1.add(new Integer(-20));
        link1.add(new Integer(8));
        //Create a reverse order compartor
        Comparator r =  Collections.reverseOrder();
        //Sort List by using the comparator
        Collections.sort(link1,r);
        //Get iteror
        Iterator li = link1.iterator();
        System.out.println("Linked List sorted in reverse: ");
        while (li.hasNext()){
            System.out.println(li.next() + " ");
        }
        //shuffle Linked List
        Collections.shuffle(link1);
        //display randomsized list
        li = link1.iterator();
        System.out.println("Linked List shufled: ");
        while (li.hasNext()){
            System.out.println(li.next() + " ");
        }
        System.out.println("Mininum:" +  ' ' + Collections.min(link1));
        System.out.println("Maxinum:" +  ' ' + Collections.max(link1));
        System.out.println("###########LinkedList Algorithms Demon end ###############################");




    }
}
