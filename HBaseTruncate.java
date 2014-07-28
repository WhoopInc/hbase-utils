// USAGE
// 
// javac -cp "$(hbase classpath)" HBaseTruncate.java

import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseTruncate {
    public static Configuration conf;

    public static void main(String[] args) throws Exception {
        conf = HBaseConfiguration.create();
        for (String tableName : args) {
            clearTable(tableName);
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
}

