package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by w00274071 on 2018/4/16.
 */
public class CEPTestExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        final ParameterTool params = ParameterTool.fromArgs(args);

        DataStream<Mutable> input = null;
        String sql = null;
        Table table = null;

        if (!params.has("sql")) {
            System.out.println("specify the sql1 or sql2.");
            return;
        }else{
            sql = params.get("sql");
        }

        System.out.println("sql param is " + sql);

        if (sql.equals("sql1")){
            input = env.fromElements(
                    new Mutable("1", "a"),
                    new Mutable("2", "b"),
                    new Mutable("3", "a"),
                    new Mutable("4", "b"),
                    new Mutable("5", "a"),
                    new Mutable("6", "b"),
                    new Mutable("7", "a"),
                    new Mutable("8", "a"),
                    new Mutable("9", "b"),
                    new Mutable("10", "a"),
                    new Mutable("11", "b"),
                    new Mutable("12", "a"),
                    new Mutable("13", "a"),
                    new Mutable("14", "b"),
                    new Mutable("15", "a"),
                    new Mutable("16", "b"),
                    new Mutable("17", "a"),
                    new Mutable("18", "b"),
                    new Mutable("19", "a"),
                    new Mutable("20", "b"),
                    new Mutable("21", "a"),
                    new Mutable("22", "b"),
                    new Mutable("23", "a"),
                    new Mutable("24", "b")
            );

            tEnv.registerDataStream("mytable", input, "num,name");

            table = tEnv.sqlQuery(
                    "SELECT * FROM mytable MATCH_RECOGNIZE " +
                            "(" +
                            "MEASURES 1 AS AName " +
                            "ONE ROW PER MATCH " +    //匹配成功输出一条。
                            "AFTER MATCH SKIP PAST LAST ROW " +  //匹配成功之后，从匹配成功的事件序列中的最后一个事件的下一个事件开始进行下一次匹配。
                            "PATTERN ((A B){3})" +
                            //	定义待识别的事件序列需要满足的规则，需要定义在()中，
                            // 由一系列自定义的patternVariable构成  {n} n次
                            "DEFINE " +  //定义在PATTERN中出现的patternVariable的具体含义，若某个patternVariable在DEFINE中没有定义，则认为对于每一个事件，该patternVariable都成立。
                            "A AS A.name = 'a', " +
                            "B AS B.name = 'b' " +
                            ")MR");
        }else if (sql.equals("sql2")){
            input = env.fromElements(
                    new Mutable("1", "a"),
                    new Mutable("2", "a"),
                    new Mutable("3", "b"),
                    new Mutable("4", "b"),
                    new Mutable("5", "b"),
                    new Mutable("6", "c"),
                    new Mutable("7", "d"),
                    new Mutable("8", "a"),
                    new Mutable("9", "a"),
                    new Mutable("10", "a"),
                    new Mutable("11", "b"),
                    new Mutable("12", "b"),
                    new Mutable("13", "b"),
                    new Mutable("14", "c")
            );

            tEnv.registerDataStream("mytable", input, "num,name");

            table = tEnv.sqlQuery(
                    "SELECT * FROM mytable MATCH_RECOGNIZE" +
                            " (" +
                            " MEASURES FIRST(B.name) as Bname" +
                            " ONE ROW PER MATCH" +
                            " AFTER MATCH SKIP PAST LAST ROW" +
                            " PATTERN (B+)" +
                            " DEFINE" +
                            " B AS B.name <> PREV(B.name) or PREV(B.name) is null " +
                            " ) MR");
        }


        tEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }

    public static class Mutable {
        public String num;
        public String name;

        public Mutable() {

        }


        public Mutable(String num, String name) {
            this.num = num;
            this.name = name;
        }

        public String toString() {
            return num + "," + name;
        }
    }

}
