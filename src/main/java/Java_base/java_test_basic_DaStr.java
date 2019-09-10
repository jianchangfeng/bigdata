package Java_base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Stack;
import java.util.Vector;
public class java_test_basic_DaStr {
    public static void main(String[] args) throws IOException {
        // Vector Demo
        Vector<Integer> v = new Vector<Integer>();
        Vector<Float> x = new Vector<Float>();
        Vector<Double> y = new Vector<Double>();
        System.out.println("Initial size:" + v.size());
        System.out.println("Initial capacity:" + v.capacity());
        v.addElement(new Integer(1));
        v.addElement(new Integer(2));
        v.addElement(new Integer(3));
        v.addElement(new Integer(4));
        v.addElement(new Integer(5));
        x.addElement(new Float(5.4));
        y.addElement(new Double(3.999));
        x.removeElement(new Float(5.4));
        y.removeElement(new Double(3.999));
        System.out.println("Initial size:" + v.size());
        System.out.println("Capacity after four additions:" + v.capacity());
        Enumeration vEnum = v.elements();
        System.out.println("\nElements in vector:");
        while (vEnum.hasMoreElements())
            System.out.print(vEnum.nextElement() + " ");
        System.out.println();

        // // Stack Demo
        Stack<Integer> st = new Stack<Integer>();
        st.push(1);
        st.push(2);
        st.push(3);
        st.push(4);
        // System.out.println(st.search(2));
        // System.out.println(st.pop());
        // System.out.println(st.pop());
        Enumeration stnum = st.elements();
        System.out.println("\nElements in stack:");
        while (stnum.hasMoreElements())
            System.out.print(stnum.nextElement() + " ");
        System.out.println();
        // // HashTalbe Demo
        Hashtable<String, Double> score = new Hashtable<String, Double>();
        score.put("Zara", new Double(91.4));
        score.put("Marry", new Double(60));
        score.put("Ana", new Double(78.2));
        System.out.println(score.get("Marry"));
        // // Properties
        // "G:\JAVA\workspace\java_test_basic\src\java_test_basic\prop.txt"
        // Properties defaultProps = new Properties();
        // FileInputStream in = new FileInputStream("prop.txt");
        // defaultProps.load(in);
        // in.close();
        // for(Object key : defaultProps.keySet()){
        // System.out.println(key + ": " + defaultProps.get(key));
        // defaultProps.setProperty((String)key, "new value");
        // }
        //
        // defaultProps.put("newprop",String.valueOf(10));
        //
        // FileOutputStream out = new FileOutputStream("appProperties");
        // defaultProps.store(out,"---No Comment---");
        // out.close();
        // //ArrayListDemo
        ArrayList al = new ArrayList();
        System.out.println("Initial size of al: " + al.size());

        //add elements to the arrary list
        al.add("C");
        al.add("A");
        al.add("E");
        al.add("B");
        al.add("D");
        al.add("F");
        al.add(1, "A2");
        System.out.println("Size of al after additions: " + al.size());

        //dispaly the array list
        System.out.println("Contents of al: " + al);
        //Remove elements from the array list
        al.remove("F");
        al.remove(2);
        System.out.println("Size of al after deletions: " + al.size());
        System.out.println("Contents of al: " + al);
        // Algorithms Demo
        LinkedList ll = new LinkedList();
        ll.add(new Integer(-8));
        ll.add(new Integer(20));
        ll.add(new Integer(-20));
        ll.add(new Integer(8));

        //Create a reverse order compartor
        Comparator r = Collections.reverseOrder();
        //Sort list by using the comparator
        Collections.sort(ll, r);
        //Get iterator
        Iterator li = ll.iterator();
        System.out.println("List sorted in reverse: ");
        while (li.hasNext()) {
            System.out.println(li.next() + " ");
        }
        System.out.println();
        Collections.shuffle(ll);
        //Display randomized list
        li = ll.listIterator();
        System.out.println("List shuffled: ");
        while (li.hasNext()) {
            System.out.println(li.next() + " ");
        }
        System.out.println();
        System.out.println("Min: " + Collections.min(ll));
        System.out.println("Max: " + Collections.max(ll));
    }
}
