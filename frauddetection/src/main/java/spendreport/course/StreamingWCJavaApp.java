package spendreport.course;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 *
 *
 * word count
 */
public class StreamingWCJavaApp {


    public static void main(String[] args) throws Exception {

        // 获取参数
        int port = 0;
        String host = "localhost";

        ParameterTool tool = ParameterTool.fromArgs(args);
        try {
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置，使用默认端口9999");
            port = 9999;
        }

        try {
            host = tool.get("host", "localhost");
        } catch (Exception e) {
            System.err.println("host未设置，使用默认hostname: localhost");
            host = "localhost";
        }

        // step1 ：Get environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(2);

        // step2：read data
        DataStreamSource<String> text = env.socketTextStream(host, port);

        // step3: transform
        DataStream<WC> textWC = text.map(new MyMapFunction());

        // step4: watermarks
        DataStream<WC> textWCWithWaterMarks = textWC.assignTimestampsAndWatermarks(
                                                    WatermarkStrategy.<WC>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                                                            .withTimestampAssigner((event, timestamp) -> event.time)
                                                );

        KeyedStream<WC, String> textKeyStream = textWCWithWaterMarks.keyBy(new KeySelector<WC, String>() {

            @Override
            public String getKey(WC wc) throws Exception {
                return wc.word;
            }
        });

        WindowedStream<WC, String, TimeWindow> windowStream = textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<WC> sumOperator = windowStream.sum("count");

        DataStreamSink<WC> sink = sumOperator.print();

//        sink.setParallelism(2);

        env.execute("StreamingWCJavaApp");
    }


    public static class MyMapFunction implements MapFunction<String, WC> {

        @Override
        public WC map(String s) throws Exception {
            String[] strs = s.toLowerCase().split(" ");
            if(strs.length != 2){
                return null;
            }
            Long time = null;
            try {
                time = Long.parseLong(strs[1].trim());
            }catch (Exception e){
                return null;
            }
            return new WC(strs[0].trim(), time, 1);
        }
    }

    public static class WC {
        private String word;
        private long time;
        private int count;

        public WC(){}

        public WC(String word, long time, int count ){
            this.word = word;
            this.count = count;
            this.time = time;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    "time='" + time + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }
}
