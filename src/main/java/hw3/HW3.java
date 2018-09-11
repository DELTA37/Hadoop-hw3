package hw3;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;
import java.net.URL;


public class HW3 extends Configured implements Tool {

    static enum Counters {
        ROBOTS_COUNTER
    }

    public static class TextPair implements WritableComparable<TextPair> {
        private Text first;
        private Text second;

        public TextPair() {
            first = new Text();
            second = new Text();
        }

        public TextPair(String _first, String _second) {
            first = new Text(_first);
            second = new Text(_second);
        }

        public TextPair(Text _first, Text _second) {
            first = _first;
            second = _second;
        }

        public Text getFirst() {
            return first;
        }

        public Text getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        @Override
        public int compareTo(@Nonnull TextPair o) {
            int a = first.compareTo(o.first);
            if (a == 0) {
                a = second.compareTo(o.second);
            }
            return a;
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            first.readFields(dataInput);
            second.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TextPair)) {
                return false;
            }
            TextPair tp = (TextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }
    } // TextPair

    public int on_finish(String[] args) throws Exception {
        Job job = GetJobConf(args);
        System.out.println("NUMBER_OF_REDUCERS = " + job.getCounters().findCounter("NUMBER_OF_REDUCERS", "num").getValue());
        System.out.println("DISALLOWED = " + job.getCounters().findCounter("DISALLOWED", "num").getValue());
        System.out.println("UPDATED_PAGES = " + job.getCounters().findCounter("UPDATED_PAGES", "num").getValue());
        System.out.println("webpages = " + job.getCounters().findCounter("webpages", "num").getValue());
        System.out.println("websites = " + job.getCounters().findCounter("websites", "num").getValue());
        return 0;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(String[] args) throws IOException {
        Job job = Job.getInstance(getConf(), "HW3Kaspar");
        job.setJarByClass(HW3.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan1 = new Scan();
        scan1.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("url"));
        scan1.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("webpages_kaspar"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("site"));
        scan2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("robots"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("websites_kaspar"));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, HW3Mapper.class, TextPair.class, Text.class, job);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        TableMapReduceUtil.initTableReducerJob(new String("pages"), HW3Reducer.class, job);
        return job;
    }

    static String getHost(String url) {
        if (url.startsWith("http")) {
            return url.split("/")[2];
        } else {
            return url.split("/")[0];
        }
    }

    static public class HW3Mapper extends TableMapper<TextPair, Text> {
        @Override
        protected void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {
            TableSplit currentSplit = (TableSplit) context.getInputSplit();
            String tableName = new String(currentSplit.getTableName());

            if (tableName.startsWith("websites")) {
                context.getCounter("websites", "num").increment(1);

                Cell robots_val = columns.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("robots"));
                String robots = (robots_val != null) ? new String(CellUtil.cloneValue(robots_val), "UTF8") : "";

                Cell site_val = columns.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("site"));
                String host = new String(CellUtil.cloneValue(site_val), "UTF8");

                context.write(new TextPair(new Text(host), new Text(new String("s"))), new Text(robots));

            } else if (tableName.startsWith("webpages")) {
                context.getCounter("webpages", "num").increment(1);

                Cell url_val = columns.getColumnLatestCell(Bytes.toBytes("docs"), Bytes.toBytes("url"));
                String url = new String(CellUtil.cloneValue(url_val), "UTF8");

                String host = getHost(url); 

                Cell disabled_val = columns.getColumnLatestCell(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                String disallowed_flag = (disabled_val != null) ? "Y" : "N";

                String mapper_value = Bytes.toString(rowKey.get()) + "\t" + (new URL(url)).getFile() + "\t" + disallowed_flag;

                context.write(new TextPair(new Text(host), new Text(new String("p"))), new Text(mapper_value));
            }
        }
    }
    

    static Pair<String, String> parse_rule(String rule) {
        if (rule.endsWith("$")) {
            return new Pair<String, String>("$", rule.substring(0, rule.length()-1));
        }

        if (rule.startsWith("/")) {
            return new Pair<String, String>("/", rule);
        }

        if (rule.startsWith("*")) {
            return new Pair<String, String>("*", rule.substring(1, rule.length()));
        }

        if (rule.startsWith("/") && rule.endsWith("$")) {
            return new Pair<String, String>("/$", rule.substring(0, rule.length()-1));
        }

        if (rule.startsWith("*") && rule.endsWith("$")) {
            return new Pair<String, String>("*$", rule.substring(1, rule.length()-1));
        }
        return new Pair<String, String>();
    }


    static boolean check_url(String url, Pair<String, String> rule) {
        if (rule.getFirst().equals("/") && url.startsWith(rule.getSecond())) {
            return true;
        } else if (rule.getFirst().equals("*") && url.contains(rule.getSecond())) {
            return true;
        } else if (rule.getFirst().equals("$") && url.endsWith(rule.getSecond())) {
            return true;
        } else if (rule.getFirst().equals("/$") && url.equals(rule.getSecond())) {
            return true;
        } else if (rule.getFirst().equals("*$") && url.endsWith(rule.getSecond())) {
            return true;
        }
        return false;
    }


    static public class HW3Reducer extends TableReducer<TextPair, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String robots = values.iterator().next().toString();
            context.getCounter("NUMBER_OF_REDUCERS", "num").increment(1);

            List<Pair<String, String>> rules = new ArrayList<Pair<String, String>>();
            if (robots.startsWith("Disallow")) {
                context.getCounter("ROBOTS_COUNTER", "num").increment(1);

                String[] robots_rules = robots.split("\n");

                for (String rule : robots_rules) {
                    String rule_str = rule.split(" ")[1].trim();
                    Pair<String, String> rulePair = parse_rule(rule_str);
                    context.getCounter("ROBOTS_WITH_" + rulePair.getFirst(), "num").increment(1);
                    rules.add(rulePair);
                }
            }

            for (Text val : values) {
                context.getCounter("TOTAL_URLS", "num").increment(1);

                String[] page_meta = val.toString().split("\t");
                String row_key = page_meta[0];
                String url = page_meta[1];
                String disallowed_flag = page_meta[2];

                boolean is_disallowed = false;
                for (Pair<String, String> r : rules) {
                    is_disallowed = check_url(url, r);
                    if (is_disallowed) {
                        context.getCounter("DISALLOWED", "num").increment(1);
                        break;
                    }
                }

                if (is_disallowed && !disallowed_flag.equals("Y")) {
                    Put put = new Put(Bytes.toBytes(row_key));
                    put.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                    context.getCounter("UPDATED_PAGES", "num").increment(1);
                    context.write(null, put);
                }

                if (!is_disallowed && disallowed_flag.equals("Y")) {
                    Delete del = new Delete(Bytes.toBytes(row_key));
                    del.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                    context.getCounter("UPDATED_PAGES", "num").increment(1);
                    context.write(null, del);
                }
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<TextPair, NullWritable> {
        @Override
        public int getPartition(TextPair key, NullWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;

            return ip1.compareTo(ip2);
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;
            return ip1.getFirst().compareTo(ip2.getFirst());
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(HBaseConfiguration.create(), new HW3(), args));
    }
}
