package cn.jannal.rocketmq;


import java.util.ArrayList;
import java.util.ListIterator;

/**
 * @author jannal
 **/
public class Test {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        // 逆序遍历
        ListIterator<String> iterator = list.listIterator(list.size());
        String result = null;
        while (iterator.hasPrevious()) {
            result = iterator.previous();
            System.out.println(result);
            //iterator.remove();
        }


    }
}
