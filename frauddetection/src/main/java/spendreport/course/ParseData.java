package spendreport.course;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.function.Consumer;

public class ParseData {
    public static void main(String[] args) throws Exception {
        String testData = "Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.\n" +
                "\n" +
                "Here, we explain important aspects of Flink’s architecture.\n" +
                "\n" +
                "Process Unbounded and Bounded Data\n" +
                "Any kind of data is produced as a stream of events. Credit card transactions, sensor measurements, machine logs, or user interactions on a website or mobile application, all of these data are generated as a stream.\n" +
                "\n" +
                "Data can be processed as unbounded or bounded streams.\n" +
                "\n" +
                "Unbounded streams have a start but no defined end. They do not terminate and provide data as it is generated. Unbounded streams must be continuously processed, i.e., events must be promptly handled after they have been ingested. It is not possible to wait for all input data to arrive because the input is unbounded and will not be complete at any point in time. Processing unbounded data often requires that events are ingested in a specific order, such as the order in which events occurred, to be able to reason about result completeness.\n" +
                "\n" +
                "Bounded streams have a defined start and end. Bounded streams can be processed by ingesting all data before performing any computations. Ordered ingestion is not required to process bounded streams because a bounded data set can always be sorted. Processing of bounded streams is also known as batch processing." +
                "Apache Flink excels at processing unbounded and bounded data sets. Precise control of time and state enable Flink’s runtime to run any kind of application on unbounded streams. Bounded streams are internally processed by algorithms and data structures that are specifically designed for fixed sized data sets, yielding excellent performance.\n" +
                "\n" +
                "Convince yourself by exploring the use cases that have been built on top of Flink.\n" +
                "\n" +
                "Deploy Applications Anywhere\n" +
                "Apache Flink is a distributed system and requires compute resources in order to execute applications. Flink integrates with all common cluster resource managers such as Hadoop YARN, Apache Mesos, and Kubernetes but can also be setup to run as a stand-alone cluster.\n" +
                "\n" +
                "Flink is designed to work well each of the previously listed resource managers. This is achieved by resource-manager-specific deployment modes that allow Flink to interact with each resource manager in its idiomatic way.\n" +
                "\n" +
                "When deploying a Flink application, Flink automatically identifies the required resources based on the application’s configured parallelism and requests them from the resource manager. In case of a failure, Flink replaces the failed container by requesting new resources. All communication to submit or control an application happens via REST calls. This eases the integration of Flink in many environments.\n" +
                "\n" +
                "Run Applications at any Scale\n" +
                "Flink is designed to run stateful streaming applications at any scale. Applications are parallelized into possibly thousands of tasks that are distributed and concurrently executed in a cluster. Therefore, an application can leverage virtually unlimited amounts of CPUs, main memory, disk and network IO. Moreover, Flink easily maintains very large application state. Its asynchronous and incremental checkpointing algorithm ensures minimal impact on processing latencies while guaranteeing exactly-once state consistency.\n" +
                "\n" +
                "Users reported impressive scalability numbers for Flink applications running in their production environments, such as\n" +
                "\n" +
                "applications processing multiple trillions of events per day,\n" +
                "applications maintaining multiple terabytes of state, and\n" +
                "applications running on thousands of cores.\n" +
                "Leverage In-Memory Performance\n" +
                "Stateful Flink applications are optimized for local state access. Task state is always maintained in memory or, if the state size exceeds the available memory, in access-efficient on-disk data structures. Hence, tasks perform all computations by accessing local, often in-memory, state yielding very low processing latencies. Flink guarantees exactly-once state consistency in case of failures by periodically and asynchronously checkpointing the local state to durable storage.";
        testData = testData.replace(",", " ");
        testData = testData.replace(".", " ");
        testData = testData.replace(";", " ");
        String[] results = testData.split("\\s|\n|\r");
        int length = results.length;
        ArrayList<String> trimedStrs = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            String r = results[i].trim();
            if(("").equals(r)){
                continue;
            }
            trimedStrs.add(r);
        }

        long sleepTime = 50000 / trimedStrs.size();
        File f=new File("./a.txt");
        FileOutputStream fos1=new FileOutputStream(f);
        final OutputStreamWriter dos1=new OutputStreamWriter(fos1);
        final long now = System.currentTimeMillis();

        trimedStrs.forEach(new Consumer<String>() {
            int index = 0;
            @Override
            public void accept(String s) {
                try {
                    dos1.write(s + " " + (now + sleepTime * index) + "\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                index++;
            }
        });
        dos1.close();
    }
}
