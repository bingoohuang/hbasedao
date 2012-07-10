package org.phw.hbasedao.cdr;

import java.io.File;
import java.util.ArrayList;

import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.pool.HTablePoolManager;

public class PutCDRDemo {
    public static void main(String[] args) throws HDaoException {
        System.out.println("Usage(V2012-07-09 FOR XUJG): cdrdemo [quorum port batchNum]");
        String quorum = "127.0.0.1";
        //String quorum = "10.142.195.67,10.142.151.88,10.142.195.63";
        if (args.length > 0) {
            quorum = args[0];
        }
        String port = "2181";
        if (args.length > 1) {
            port = args[1];
        }

        int batchNum = 1000;
        if (args.length > 2) {
            batchNum = Integer.valueOf(args[2]);
        }

        HTablePoolManager.createHBaseConfiguration("BJHBASE", quorum, port);
        RecordsGenerator recordsGenerator = new RecordsGenerator();
        CallRecordDetail randomRecord = recordsGenerator.randomRecord(null);
        HDao hdao = new DefaultHDao("BJHBASE");
        ArrayList<CallRecordDetail> records = new ArrayList<CallRecordDetail>(batchNum);
        CdrBatch cdrBatch = new CdrBatch();
        long startTime = System.currentTimeMillis();
        long c = 0;
        File stopFile = new File("stop");

        for (long k = 0; k < Long.MAX_VALUE; ++k) {
            if (c == 0) {
                cdrBatch.setTimestamp(randomRecord.getTimestamp());
                cdrBatch.setStart(randomRecord.getDesc());
                hdao.put(cdrBatch);
                System.out.print("Timestamp:" + randomRecord.getTimestamp() + ", Start:" + randomRecord.getDesc());
            }

            randomRecord = recordsGenerator.randomRecord(randomRecord);
            records.add(randomRecord);
            ++c;
            if (c == batchNum) {
                c = 0;
                hdao.put(records);
                records.clear();

                long endTime = System.currentTimeMillis();
                System.out.println(", " + batchNum + " Reords, End:" + randomRecord.getDesc() + ", Cost:"
                        + (endTime - startTime));
                cdrBatch.setEnd(randomRecord.getDesc());
                hdao.put(cdrBatch);
                startTime = System.currentTimeMillis();

                if (stopFile.exists()) {
                    stopFile.deleteOnExit();
                    break;
                }
            }
        }

        System.out.println("Exited! ");
    }
}
