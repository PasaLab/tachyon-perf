package tachyon.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.datagen.ListGenerator;
import tachyon.perf.thread.PerfThread;
import tachyon.perf.thread.ReadThread;
import tachyon.perf.thread.WriteThread;

/**
 * Entry point for a tachyon-perf process
 */
public class TachyonPerf {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length != 3) {
      LOG.error("Wrong program arguments. Should be <NODEID> <Read|Write> <READTYPE|WRITETYPE>. "
          + "See more in bin/tachyon-perf");
      System.exit(-1);
    }
    TachyonPerf perf;
    try {
      perf = new TachyonPerf(args[0], args[1], args[2]);
      perf.setupTachyonPerf();
    } catch (IOException e) {
      LOG.error("Failed to setup TachyonPerf", e);
      throw new RuntimeException(e);
    }
    LOG.info("Starting all the test threads...");
    perf.start();

    try {
      perf.waitAllThreads();
    } catch (InterruptedException e) {
      LOG.error("Error when wait test threads", e);
      throw new RuntimeException(e);
    }

    LOG.info("All threads completed. Generating report...");
    try {
      perf.generateReport();
    } catch (IOException e) {
      LOG.error("Error when generate report", e);
    }

    try {
      perf.success();
    } catch (IOException e) {
      LOG.error("Error when create success file", e);
    }
  }

  private final PerfConf PERF_CONF;
  private final long START_TIME;

  private final int ID;
  private final TestType TEST_TYPE;
  private final ReadType READ_TYPE;
  private final WriteType WRITE_TYPE;

  private PerfThread[] mPerfThreads;
  private List<Thread> mTestThreads;

  private TestReport mReport;

  /**
   * Create a new TachyonPerf
   * 
   * @param nodeid
   *          the id of this TachyonPerf
   * @param testType
   *          the string format of the test type, Read or Write
   * @param RWType
   *          the string format of the read or write type. It's related to tachyon.client
   * @throws IOException
   */
  public TachyonPerf(String nodeid, String testType, String RWType) throws IOException {
    PERF_CONF = PerfConf.get();
    START_TIME = System.currentTimeMillis();
    ID = Integer.parseInt(nodeid);
    TEST_TYPE = TestType.getTestType(testType);
    if (TEST_TYPE.isRead()) {
      READ_TYPE = ReadType.getOpType(RWType);
      WRITE_TYPE = null;
    } else if (TEST_TYPE.isWrite()) {
      READ_TYPE = null;
      WRITE_TYPE = WriteType.getOpType(RWType);
    } else {
      throw new IOException("Unknown TestType");
    }
  }

  private void setupReadTest(TachyonFS tfs) throws IOException {
    String readDir = PERF_CONF.TFS_DIR + "/" + ID;

    if (!tfs.exist(readDir)) {
      throw new IOException("The read dir " + readDir + " is not exist. "
          + "Do the write test fisrt");
    }

    if (tfs.getFile(readDir).isFile()) {
      throw new IOException("The read dir " + readDir + " is not a directory. "
          + "Do the write test fisrt");
    }

    List<Integer> readFileCandidates = tfs.listFiles(readDir, true);
    if (readFileCandidates.isEmpty()) {
      throw new IOException("The read dir " + readDir + " is empty");
    }

    LOG.info("The read dir is " + readDir + ", contains " + readFileCandidates.size() + " files");
    ReadMode readMode = ReadMode.getReadMode(PERF_CONF.READ_MODE);

    int threadsNum = PERF_CONF.READ_THREADS_NUM;
    List<Integer>[] readFileList =
        ListGenerator.generateReadFiles(threadsNum, PERF_CONF.READ_FILES_PER_THREAD,
            readFileCandidates, readMode, PERF_CONF.READ_IDENTICAL);
    mPerfThreads = new PerfThread[threadsNum];
    for (int i = 0; i < threadsNum; i ++) {
      mPerfThreads[i] = new ReadThread(i, PERF_CONF.TFS_ADDRESS, readFileList[i], READ_TYPE);
    }
    LOG.info("Create " + threadsNum + " read test thread");
  }

  private void setupWriteTest(TachyonFS tfs) throws IOException {
    String writeDir = PERF_CONF.TFS_DIR + "/" + ID;
    if (tfs.exist(writeDir)) {
      tfs.delete(writeDir, true);
      LOG.warn("The write dir " + writeDir + " already exists, delete it");
    }
    tfs.mkdir(writeDir);
    LOG.info("Create the write dir " + writeDir);

    int threadsNum = PERF_CONF.WRITE_THREADS_NUM;
    List<String>[] writeFileList =
        ListGenerator.generateWriteFiles(threadsNum, PERF_CONF.WRITE_FILES_PER_THREAD, writeDir);
    mPerfThreads = new PerfThread[threadsNum];
    for (int i = 0; i < threadsNum; i ++) {
      mPerfThreads[i] =
          new WriteThread(i, PERF_CONF.TFS_ADDRESS, writeFileList[i], WRITE_TYPE,
              PERF_CONF.FILE_LENGTH);
    }
    LOG.info("Create " + threadsNum + " write test thread");
  }

  /**
   * Generate the test report and output it to the configured OUT_DIR
   * 
   * @throws IOException
   */
  public void generateReport() throws IOException {
    String rwType;
    if (TEST_TYPE.isRead()) {
      rwType = READ_TYPE.name();
    } else {
      rwType = WRITE_TYPE.name();
    }
    mReport = new TestReport(START_TIME, mPerfThreads, TEST_TYPE, rwType);
    mReport.generateReport();
  }

  /**
   * Setup test. Check all the configuration and do the initialization
   * 
   * @throws IOException
   */
  public void setupTachyonPerf() throws IOException {
    String tfsWorkDir = PERF_CONF.TFS_ADDRESS + PERF_CONF.TFS_DIR;
    TachyonFS tfs = TachyonFS.get(tfsWorkDir);

    String tfsSuccessPath = PERF_CONF.TFS_DIR + "/" + ID + "/SUCCESS";
    if (tfs.exist(tfsSuccessPath)) {
      tfs.delete(tfsSuccessPath, true);
    }

    if (TEST_TYPE.isRead()) {
      setupReadTest(tfs);
    } else if (TEST_TYPE.isWrite()) {
      setupWriteTest(tfs);
    }
    try {
      tfs.close();
    } catch (TException e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  /**
   * Start all the test threads
   */
  public void start() {
    mTestThreads = new ArrayList<Thread>(mPerfThreads.length);
    for (int i = 0; i < mPerfThreads.length; i ++) {
      Thread perfThread = new Thread(mPerfThreads[i]);
      mTestThreads.add(perfThread);
      perfThread.start();
    }
  }

  public void success() throws IOException {
    TachyonFS tfs = TachyonFS.get(PERF_CONF.TFS_ADDRESS);
    tfs.createFile(PERF_CONF.TFS_DIR + "/" + ID + "/SUCCESS");
  }

  /**
   * Wait for the test threads. It will return until all the test threads are dead.
   * 
   * @throws InterruptedException
   */
  public void waitAllThreads() throws InterruptedException {
    for (Thread thread : mTestThreads) {
      thread.join();
    }
  }
}
