package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapreduce.Mapper;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.array.ArrayListWritable;
import java.util.NavigableMap;

import java.io.IOException;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;


public class BooleanRetrievalHBase extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BooleanRetrievalHBase.class);

  public static final String[] FAMILIES = { "p" };
  public static final byte[] PF = FAMILIES[0].getBytes();

  // TODO: which of these are needed?
  private Table indexTable;
  private Table collectionTable;
  private Stack<Set<Integer>> stack;

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

    private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (byte[] docno : fetchPostings(term)) {
      set.add(Bytes.toInt(docno));
    }

    return set;
  }

  private Set<byte[]> fetchPostings(String term) throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    Result result = indexTable.get(get);
    NavigableMap<byte[],byte[]> map = result.getFamilyMap(PF);

    return map.keySet();
  }

  public String fetchLine(long offset) throws IOException {
    Get get = new Get(Bytes.toBytes(offset));
    Result result = collectionTable.get(get);
    String d = Bytes.toString(result.getValue(InsertCollectionHBase.CF, InsertCollectionHBase.COLLECTION));

    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  public BooleanRetrievalHBase() {}

  public static class Args {
    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "input collection")
    public String collection;

    @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;
  }

  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BooleanRetrievalHBase.class.getSimpleName());
    LOG.info(" - collection: " + args.collection);
    LOG.info(" - index table: " + args.table);
    LOG.info(" - config: " + args.config);
    LOG.info(" - query: " + args.query);


    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);
    indexTable = connection.getTable(TableName.valueOf(args.table));
    collectionTable = connection.getTable(TableName.valueOf(args.collection));

    stack = new Stack<>();

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 0;
  }

    public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalHBase(), args);
  }

}
