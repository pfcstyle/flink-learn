package spendreport.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Michael PK
 */
public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // fromCollection(env);
        textFile(env);
    }


    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///F:/Code/IdeaProjects/frauddetection/src/main/resources/Hello.txt";

        env.readTextFile(filePath).print();

        System.out.println("~~~~~~~华丽的分割线~~~~~~~~");

        filePath = "file:///F:/Code/IdeaProjects/frauddetection/src/main/resources";
        env.readTextFile(filePath).print();
    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for(int i=1; i<=10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}

