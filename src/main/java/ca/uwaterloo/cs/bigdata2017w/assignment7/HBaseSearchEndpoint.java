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
import java.util.Arrays;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;


public class HBaseSearchEndpoint extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(HBaseSearchEndpoint.class);

  public static String[] retrieval_args;

  public HBaseSearchEndpoint() {}

  public static class Args {
    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "input collection")
    public String collection;

    @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

    @Option(name = "-port", metaVar = "[term]", required = true, usage = "port")
    public Integer port;
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

    LOG.info("Tool: " + HBaseSearchEndpoint.class.getSimpleName());
    LOG.info(" - collection: " + args.collection);
    LOG.info(" - index table: " + args.table);
    LOG.info(" - config: " + args.config);
    LOG.info(" - port: " + args.port);

    // If the table doesn't already exist, create it.
    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);

    retrieval_args = new String[8];
    retrieval_args[0] = "-collection";
    retrieval_args[1] = args.collection;
    retrieval_args[2] = "-index";
    retrieval_args[3] = args.table;
    retrieval_args[4] = "-config";
    retrieval_args[5] = args.config;

    Server server = new Server(args.port);
    ServletContextHandler handler = new ServletContextHandler(server, "");
    handler.addServlet(SearchServlet.class, "/search");
    server.start();

    return 0;
  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HBaseSearchEndpoint(), args);
  }
}