package org.phw.hbasedao.cdr;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.pool.HTablePoolManager;

public class PutCDRDemo {
    private static volatile int threadNum = 0;
    private static volatile int targetNum = 0;
    private static volatile boolean verbose = true;
    private static volatile Object verboseLock = new Object();
    private static Random random = new SecureRandom();

    /**
     * 文件命令。
     * 1)退出应用: echo quit>stop
     * 2)增加10个线程: echo +10>stop
     * 3)减少10个线程: echo -10>stop
     * 4)设置刚好10个线程: echo 10>stop
     */
    public static void main(String[] args) throws HDaoException, IOException {
        System.err.println("Usage(V2012-07-09 FOR XUJG): cdrdemo [quorum port batchNum threadNum]");
        String quorum = "10.20.16.32,10.20.16.35,10.20.16.36";
        if (args.length > 0) {
            quorum = args[0];
        }
        String port = "2181";
        if (args.length > 1) {
            port = args[1];
        }

        final int batchNum = args.length > 2 ? Integer.valueOf(args[2]) : 1000;

        threadNum = 1000;
        if (args.length > 3)
            threadNum = Integer.valueOf(args[3]);

        targetNum = threadNum;

        HTablePoolManager.createHBaseConfiguration(HTablePoolManager.DEFAULT_INSTANCE, quorum, port);
        File stopFile = new File("stop");

        //        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < threadNum; ++i)
            new Thread(new CdrThread(batchNum)).start();

        FileUtils.write(new File("status"), "Threads:" + threadNum);

        Pattern numCommandPattern = Pattern.compile("[+-]?(\\d+)");

        while (true) {
            while (!stopFile.exists())
                Threads.sleep(1000);

            String command = FileUtils.readFileToString(stopFile);
            stopFile.delete();

            command = command.trim();
            syncPrint("Command", command);

            if ("quit".equals(command)) break;

            if (verboseCommand(command)) continue;

            adjustThreadNum(batchNum, numCommandPattern, command);
        }

        synchronized (PutCDRDemo.class) {
            targetNum = 0;
        }

        syncPrint("Main Exited! ");
    }

    protected static boolean verboseCommand(String command) {
        if ("echo".equals(command) || "echo on".equals(command)) {
            synchronized (verboseLock) {
                verbose = true;
            }
            return true;
        }

        if ("echo off".equals(command)) {
            synchronized (verboseLock) {
                verbose = false;
            }
            return true;
        }

        return false;
    }

    protected static boolean adjustThreadNum(final int batchNum, Pattern numCommandPattern, String command)
            throws IOException {
        Matcher matcher = numCommandPattern.matcher(command);
        if (!matcher.find()) return false;

        int adjustNum = Integer.parseInt(matcher.group(1));
        synchronized (PutCDRDemo.class) {
            if (matcher.group().startsWith("-")) targetNum -= adjustNum;
            else if (matcher.group().startsWith("+")) targetNum += adjustNum;
            else targetNum = adjustNum;

            if (targetNum < 0) targetNum = 0;

            for (int i = 0, ii = targetNum - threadNum; i < ii; ++i) {
                new Thread(new CdrThread(batchNum)).start();
                ++threadNum;
            }

            FileUtils.write(new File("status"), "Threads:" + threadNum);
        }

        return true;
    }

    public static class CdrThread implements Runnable {
        private int batchNum;

        public CdrThread(int batchNum) {
            this.batchNum = batchNum;
        }

        @Override
        public void run() {
            syncPrint("Thread starting!");
            Thread.currentThread().setName("HbaseClientThread");
            try {
                batchIntoDb(batchNum);
            } catch (HDaoException e) {
                e.printStackTrace();
            } finally {
                syncPrint("Thread exiting!");
                synchronized (PutCDRDemo.class) {
                    --threadNum;
                    try {
                        FileUtils.write(new File("status"), "Threads:" + threadNum);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    protected static void batchIntoDb(int batchNum) throws HDaoException {
        RecordsGenerator recordsGenerator = new RecordsGenerator();
        CallRecordDetail randomRecord = null;
        HDao hdao = new DefaultHDao();
        long startTime = System.currentTimeMillis();
        long start = 0;
        long c = 0;

        for (long k = 0, kk = 1000L + random.nextInt(10000); !Thread.currentThread().isInterrupted() && k < kk; ++k) {
            // if (detectExit()) return;

            randomRecord = recordsGenerator.randomRecord(randomRecord);
            hdao.put(randomRecord);

            if (c == 0) {
                start = randomRecord.getDesc();
            }

            if (++c == batchNum) {
                c = 0;

                long end = System.currentTimeMillis();
                syncPrint("Timestamp", randomRecord.getTimestamp(), "Start", start, "Records",
                        batchNum, "End", randomRecord.getDesc(), "Cost", end - startTime);

                startTime = end;
            }
        }
    }

    /*
    private static boolean detectExit() {
        synchronized (PutCDRDemo.class) {
            if (threadNum > targetNum) {
                --threadNum;
                return true;
            }
        }

        return false;
    }
    */

    private static void syncPrint(Object... msgParts) {
        synchronized (verboseLock) {
            if (verbose) System.out.println(Arrays.toString(msgParts));
        }
    }
}
