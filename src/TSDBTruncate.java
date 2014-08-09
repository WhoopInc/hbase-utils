import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class TSDBTruncate {
    public static Configuration conf;
    public static HBaseAdmin admin;

    private static class Table {
      private String name;
      private boolean vmax = false;
      private ArrayList<String> columns = new ArrayList<String>();
      private HTableDescriptor tableDesc;
      private HBaseAdmin admin;
      private Configuration config;

      public Table(Configuration config, HBaseAdmin admin) {
        this.config = config;
        this.admin = admin;
      }

      public Table name(String name) {
        this.name = name;
        return this;
      }
      public String name() {
        return name;
      }
      public Table vmax(boolean vmax) {
        this.vmax = vmax;
        return this;
      }
      public Table column(String column) {
        this.columns.add(column);
        return this;
      }
      public Table admin(HBaseAdmin admin) {
        this.admin = admin;
        return this;
      }
      public Table config(Configuration config) {
        this.config = config;
        return this;
      }

      private Table create() throws IOException {
        admin.createTable(tableDesc);
        return this;
      }
      private Table build() {
        this.tableDesc = new HTableDescriptor(name);
        for (String column : columns) {
          createColumn(column, vmax);
        }
        return this;
      }
      private Table createColumn(String name, boolean vmax) {
        HColumnDescriptor column = new HColumnDescriptor(name);
        column.setCompressionType(Compression.Algorithm.LZO);
        column.setBloomFilterType(StoreFile.BloomType.ROW);
        if (vmax) {
          column.setMaxVersions(1);
        }
        tableDesc.addFamily(column);
        return this;
      }
      private Table clear() throws IOException {
        HTable table = new HTable(config, name);
        Iterator<Result> scanner = table.getScanner(new Scan()).iterator();

        while (scanner.hasNext()) {
          Delete delete = new Delete(scanner.next().getRow());
          table.delete(delete);
        }
        return this;
      }
      public Table setup() throws IOException {
        if (admin.tableExists(name)) {
          return clear();
        } else {
          return build().create();
        }
      }
    }

    public static ArrayList<Table> parseArgs(String[] args, HBaseAdmin admin, Configuration conf) {
      ArrayList<Table> tables = new ArrayList<Table>();
      if (args[0].equals("--default")) {
        System.out.println("Creating TSDB tables with default names");
        tables.add(new Table(conf, admin).name("tsdb-uid").vmax(false).column("id").column("name"));
        tables.add(new Table(conf, admin).name("tsdb").vmax(true).column("t"));
        tables.add(new Table(conf, admin).name("tsdb-tree").vmax(true).column("t"));
        tables.add(new Table(conf, admin).name("tsdb-meta").vmax(false).column("name"));
      } else {
        Table cur = null;
        for (String arg : args) {
          if (arg.equals("--table")) {
            if (cur != null) {
              tables.add(cur);
            }
            cur = new Table(conf, admin);
          } else if (cur == null) {
            System.err.println("Must specify at least one table");
            System.exit(1);
          } else if (arg.equals("--vmax")) {
            cur.vmax(true);
          } else if (arg.equals("--no-vmax")) {
            cur.vmax(false);
          } else if (arg.startsWith("--")) {
            System.err.println("Unknown option " + arg);
            System.exit(1);
          } else if (cur.name() == null) {
            cur.name(arg);
          } else {
            cur.column(arg);
          }
        }
        tables.add(cur);
      }
      return tables;
    }

    public static void main(String[] args) throws Exception {
      if (args.length < 1) {
        System.err.println("usage: TSDBTruncate <--default | --table <tablename> [--vmax] <col1> [col2] ... --table <table2> ...>");
        System.exit(1);
      }

      Logger.getLogger("org.apache.zookeeper.ClientCnxn").setLevel(Level.WARN);
      Logger.getLogger("org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper").setLevel(Level.WARN);
      Logger.getLogger("org.apache.hadoop.hbase.catalog.CatalogTracker").setLevel(Level.WARN);
      Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.WARN);

      Configuration conf = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(conf);

      ArrayList<Table> tables = parseArgs(args, admin, conf);

      for (Table table : tables) {
        table.setup();
      }
    }
}
