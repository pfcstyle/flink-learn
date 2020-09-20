package spendreport.tablesql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Author: Michael PK
 */
public class BlinkTableSQLAPI {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // ******************
        // BLINK BATCH QUERY
        // ******************
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

        // table is the result of a simple projection query
        Table projTable = bsTableEnv.from("X").select("select 1 = 1");

// register the Table projTable as table "projectedTable"
        bsTableEnv.createTemporaryView("projectedTable", projTable);
    }

    public static class Sales{
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double  amountPaid;

    }
}
