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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
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
import io.bespin.java.util.Tokenizer;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;


public class InsertCollectionHBase extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(InsertCollectionHBase.class);

  public static final String[] FAMILIES = { "c" };
  public static final byte[] CF = FAMILIES[0].getBytes();
  public static final byte[] COLLECTION = "collection".getBytes();

  private static final class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      // identity mapper
      context.write(docno, doc);
    }
  }
  
  public static class MyTableReducer extends TableReducer<LongWritable, Text, ImmutableBytesWritable>  {

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      Iterator<Text> iter = values.iterator();
      Put put = new Put(Bytes.toBytes(key.get()));

      while (iter.hasNext()) {
        Text doc = iter.next();
        put.addColumn(CF, COLLECTION, Bytes.toBytes(doc.toString()));
      }
      context.write(null, put);
    }
  }

  public InsertCollectionHBase() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
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

    LOG.info("Tool: " + InsertCollectionHBase.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output table: " + args.table);
    LOG.info(" - config: " + args.config);
    LOG.info(" - number of reducers: " + args.numReducers);

    // If the table doesn't already exist, create it.
    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);
    Admin admin = connection.getAdmin();

    if (admin.tableExists(TableName.valueOf(args.table))) {
      LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.table));
      LOG.info(String.format("Disabling table '%s'", args.table));
      admin.disableTable(TableName.valueOf(args.table));
      LOG.info(String.format("Droppping table '%s'", args.table));
      admin.deleteTable(TableName.valueOf(args.table));
    }

    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.table));
    for (int i = 0; i < FAMILIES.length; i++) {
      HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
      tableDesc.addFamily(hColumnDesc);
    }
    admin.createTable(tableDesc);
    LOG.info(String.format("Successfully created table '%s'", args.table));

    admin.close();

    // Now we're ready to start running MapReduce.
    Job job = Job.getInstance(conf);
    job.setJobName(InsertCollectionHBase.class.getSimpleName());
    job.setJarByClass(InsertCollectionHBase.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    TableMapReduceUtil.initTableReducerJob(args.table, MyTableReducer.class, job);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

    public static void main(String[] args) throws Exception {
    ToolRunner.run(new InsertCollectionHBase(), args);
  }

}
