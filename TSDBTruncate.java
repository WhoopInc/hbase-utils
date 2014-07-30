import java.util.Iterator;
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

public class TSDBTruncate {
    public static Configuration conf;
    public static HBaseAdmin admin;

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.err.println("usage: TSDBTruncate <tsdb-table> <uid-table>");
            System.exit(1);
        }

        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);


        String tsdbTable = args[0];
        String uidTable = args[1];

        if (admin.tableExists(tsdbTable)) {
            clearTable(tsdbTable);
        } else {
            createTSDBTable(tsdbTable);
        }

        if (admin.tableExists(uidTable)) {
            clearTable(uidTable);
        } else {
            createUIDTable(uidTable);
        }
    }

    public static void clearTable(String tableName) throws Exception {
        HTable table = new HTable(conf, tableName);
        Iterator<Result> scanner = table.getScanner(new Scan()).iterator();

        while (scanner.hasNext()) {
          Delete delete = new Delete(scanner.next().getRow());
          table.delete(delete);
        }
    }

    public static void createTSDBTable(String tableName) throws Exception {
        HTableDescriptor table = new HTableDescriptor(tableName);
        
        HColumnDescriptor column = new HColumnDescriptor("t");
        column.setMaxVersions(1);
        column.setCompressionType(Compression.Algorithm.LZO);
        column.setBloomFilterType(StoreFile.BloomType.ROW);

        table.addFamily(column);
        admin.createTable(table);
    }

    public static void createUIDTable(String tableName) throws Exception {
        HTableDescriptor table = new HTableDescriptor(tableName);
        
        HColumnDescriptor columnId = new HColumnDescriptor("id");
        columnId.setMaxVersions(1);
        columnId.setCompressionType(Compression.Algorithm.LZO);
        columnId.setBloomFilterType(StoreFile.BloomType.ROW);

        HColumnDescriptor columnName = new HColumnDescriptor("name");
        columnName.setMaxVersions(1);
        columnName.setCompressionType(Compression.Algorithm.LZO);
        columnName.setBloomFilterType(StoreFile.BloomType.ROW);

        table.addFamily(columnId);
        table.addFamily(columnName);
        admin.createTable(table);
    }
}

