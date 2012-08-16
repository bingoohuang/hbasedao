package org.phw.hbasedao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class GetTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.122");
        // conf.set("zookeeper.znode.parent", "/hbase");

        HTable table = new HTable(conf, "TestTableA");
        HTableDescriptor hd = new HTableDescriptor("TestTable");
        System.out.println(hd.getFamilies());

        //      byte[] startRow = Bytes.toBytes("mail01");
        //      byte[] stopRow = Bytes.toBytes("mail03");

        Scan scan = new Scan();
        //      scan.setStartRow(startRow);
        //      scan.setStopRow(stopRow);

        ResultScanner scanner = table.getScanner(scan);
        Result result;

        while ((result = scanner.next()) != null) {
            System.out.println(result);
        }
        
        table.close();
    }
}
