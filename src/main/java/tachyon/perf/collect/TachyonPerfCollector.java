package tachyon.perf.collect;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

/**
 * This class is used to generate an html report
 */
public class TachyonPerfCollector {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Wrong program arguments.");
      System.exit(-1);
    }

    TachyonPerfCollector perfCollector = new TachyonPerfCollector(args[0], args[1]);
    perfCollector.generateHtmlReport();
  }

  private final PerfConf PERF_CONF;
  private final String REPORT_DIR;
  private final String WEB_RESOURCE_DIR;

  private StringBuffer mHtmlContent;
  private long mStartTimeMs = Long.MAX_VALUE;
  private boolean mSuccess = true;
  private String mReadType;
  private String mWriteType;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;
  private List<Float[]> mReadThroughput;
  private List<Float[]> mWriteThroughput;

  public TachyonPerfCollector(String reportDir, String webResourceDir) {
    PERF_CONF = PerfConf.get();
    REPORT_DIR = reportDir;
    WEB_RESOURCE_DIR = webResourceDir;
    mHtmlContent = new StringBuffer();
    try {
      initialWebResource();
    } catch (IOException e) {
      System.err.println("Failed to initialize web resource.");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void initialWebResource() throws IOException {
    File htmlFrameFile = new File(WEB_RESOURCE_DIR + "/frame.html");
    BufferedReader frameInput = new BufferedReader(new FileReader(htmlFrameFile));
    String line = frameInput.readLine();
    while (line != null) {
      mHtmlContent.append(line + "\n");
      line = frameInput.readLine();
    }
    frameInput.close();
  }

  private boolean collectData() {
    mNodes = new ArrayList<String>();
    mAvaliableCores = new ArrayList<Integer>();
    mWorkerMemory = new ArrayList<Long>();
    mReadThroughput = new ArrayList<Float[]>();
    mWriteThroughput = new ArrayList<Float[]>();

    File reportFileDir = new File(REPORT_DIR + "/nodes");
    if (!reportFileDir.isDirectory()) {
      System.err.println("Failed to collect data. Make sure run both write and read tests.");
      return false;
    }
    File[] reportFiles = reportFileDir.listFiles();
    Set<String> nodes = new HashSet<String>();
    for (File reportFile : reportFiles) {
      String[] parts = reportFile.getName().split("_");
      nodes.add(parts[3]);
    }

    try {
      for (String node : nodes) {
        File readReportFile = new File(REPORT_DIR + "/nodes/node_report_Read_" + node);
        File writeReportFile = new File(REPORT_DIR + "/nodes/node_report_Write_" + node);
        mNodes.add(node);
        loadSingleReportFile(readReportFile, true);
        loadSingleReportFile(writeReportFile, false);
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  private void loadSingleReportFile(File reportFile, boolean isReadReport) throws IOException {
    BufferedReader reportInput = new BufferedReader(new FileReader(reportFile));
    int cores = Integer.parseInt(reportInput.readLine());
    if (!isReadReport) {
      mAvaliableCores.add(cores);
    }

    long memoryBytes = Long.parseLong(reportInput.readLine());
    if (!isReadReport) {
      mWorkerMemory.add(memoryBytes);
    }

    if (isReadReport) {
      mReadType = reportInput.readLine();
    } else {
      mWriteType = reportInput.readLine();
    }

    long startTime = Long.parseLong(reportInput.readLine());
    if (startTime < mStartTimeMs) {
      mStartTimeMs = startTime;
    }

    int threadNum = Integer.parseInt(reportInput.readLine());
    Float[] throughput = new Float[threadNum];
    for (int i = 0; i < threadNum; i ++) {
      long bytes = Long.parseLong(reportInput.readLine());
      long time = Long.parseLong(reportInput.readLine());
      throughput[i] = bytes / 1024.0f / 1024.0f / (time / 1000.0f);
    }
    if (isReadReport) {
      mReadThroughput.add(throughput);
    } else {
      mWriteThroughput.add(throughput);
    }

    reportInput.close();
  }

  public void generateHtmlReport() {
    mSuccess = collectData();
    String totalState = "";
    String nodesInfo = "";
    String perfConf = "";
    String nodesThroughput = "";
    String throughputData = "";
    if (!mSuccess) {
      totalState =
          "\n<h2>Tachyon-Perf Job ID : " + mStartTimeMs + "</h2>\n<h3>Job Status: " + "Failed"
              + "</h3>\n";
    } else {
      totalState =
          "\n<h2>Tachyon-Perf Job ID : " + mStartTimeMs + "</h2>\n<h3>Job Status: "
              + "Finished Successfully" + "</h3>\n";
      nodesInfo = generateNodesInfo();
      perfConf = generatePerfConf();
      nodesThroughput = generateNodesThroughput();
      throughputData = generateThroughputData();
    }
    File htmlReportFile = new File(REPORT_DIR + "/webreport/report.html");
    try {
      FileOutputStream htmlOutput = new FileOutputStream(htmlReportFile);
      String finalReport =
          mHtmlContent.toString().replace("$$TOTAL_STATE", totalState)
              .replace("$$NODES_INFO", nodesInfo).replace("$$PERF_CONF", perfConf)
              .replace("$$NODES_THROUGHPUT", nodesThroughput)
              .replace("$$THROUGHPUT_DATA", throughputData);
      htmlOutput.write(finalReport.getBytes());
      htmlOutput.close();
    } catch (IOException e) {
      System.err.println("Failed to write report data.");
      e.printStackTrace();
    }
  }

  private String generateNodesInfo() {
    StringBuffer sbNodesInfo = new StringBuffer("\n");
    int totalCores = 0;
    long totalBytes = 0;
    for (int i = 0; i < mNodes.size(); i ++) {
      sbNodesInfo.append("<tr>\n").append("\t<td>" + mNodes.get(i) + "</td>\n")
          .append("\t<td>" + mAvaliableCores.get(i) + "</td>\n")
          .append("\t<td>" + PerfConstants.parseSizeByte(mWorkerMemory.get(i)) + "</td>\n")
          .append("</tr>\n");
      totalCores += mAvaliableCores.get(i);
      totalBytes += mWorkerMemory.get(i);
    }
    sbNodesInfo.append("<tr>\n").append("\t<td><b>Total</b></td>\n")
        .append("\t<td>" + totalCores + "</td>\n")
        .append("\t<td>" + PerfConstants.parseSizeByte(totalBytes) + "</td>\n").append("</tr>");
    return sbNodesInfo.toString();
  }

  private String generatePerfConf() {
    StringBuffer sbPerfConf = new StringBuffer("\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.tfs.address" + "</td>\n\t<td>"
        + PERF_CONF.TFS_ADDRESS + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.files.per.thread" + "</td>\n\t<td>"
        + PERF_CONF.READ_FILES_PER_THREAD + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.identical" + "</td>\n\t<td>"
        + PERF_CONF.READ_IDENTICAL + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.mode" + "</td>\n\t<td>"
        + PERF_CONF.READ_MODE + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.threads.num" + "</td>\n\t<td>"
        + PERF_CONF.READ_THREADS_NUM + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.file.length.bytes" + "</td>\n\t<td>"
        + PERF_CONF.FILE_LENGTH + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.files.per.thread" + "</td>\n\t<td>"
        + PERF_CONF.WRITE_FILES_PER_THREAD + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.threads.num" + "</td>\n\t<td>"
        + PERF_CONF.WRITE_THREADS_NUM + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "READ_TYPE" + "</td>\n\t<td>" + mReadType
        + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "WRITE_TYPE" + "</td>\n\t<td>" + mWriteType
        + "</td>\n</tr>\n");
    return sbPerfConf.toString();
  }

  private String generateNodesThroughput() {
    StringBuffer sbNodesThroughput = new StringBuffer("\n");
    for (int i = 0; i < mNodes.size(); i ++) {
      sbNodesThroughput.append("<tr>\n")
          .append("\t<th>" + mNodes.get(i) + " <br>(each row represents a thread)</th>\n")
          .append("\t<th id=\"svg" + (2 * i) + "\"></th>\n")
          .append("\t<th id=\"svg" + (2 * i + 1) + "\"></th>\n").append("</tr>\n");
    }
    return sbNodesThroughput.toString();
  }

  private String generateThroughputData() {
    StringBuffer sbReadData = new StringBuffer("\nvar totalReadData = [\n");
    StringBuffer sbWriteData = new StringBuffer("\nvar totalWriteData = [\n");
    StringBuffer sbNodesData = new StringBuffer("\nvar nodesData = [\n");

    List<Float> nodeReadThroughput = new ArrayList<Float>();
    List<Float> nodeWriteThroughput = new ArrayList<Float>();

    for (int i = 0; i < mNodes.size(); i ++) {
      float totalReadThroughput = 0;
      float totalWriteThroughput = 0;

      Float[] allReadThroughput = mReadThroughput.get(i);
      sbNodesData.append("\t[");
      for (float readThroughput : allReadThroughput) {
        totalReadThroughput += readThroughput;
        sbNodesData.append(formatFloat(readThroughput, 2) + ",");
      }
      sbNodesData.deleteCharAt(sbNodesData.length() - 1);
      sbNodesData.append("],\n");
      nodeReadThroughput.add(totalReadThroughput);

      Float[] allWriteThroughput = mWriteThroughput.get(i);
      sbNodesData.append("\t[");
      for (float writeThroughput : allWriteThroughput) {
        totalWriteThroughput += writeThroughput;
        sbNodesData.append(formatFloat(writeThroughput, 2) + ",");
      }
      sbNodesData.deleteCharAt(sbNodesData.length() - 1);
      sbNodesData.append("],\n");
      nodeWriteThroughput.add(totalWriteThroughput);
    }
    sbNodesData.append("];\n");

    for (int i = 0; i < mNodes.size(); i ++) {
      sbReadData.append("\t{\"State\": \"" + mNodes.get(i) + "\", ").append(
          "\"Read Throughput\": \"" + formatFloat(nodeReadThroughput.get(i), 2) + "\"},\n");
      sbWriteData.append("\t{\"State\": \"" + mNodes.get(i) + "\", ").append(
          "\"Write Throughput\": \"" + formatFloat(nodeWriteThroughput.get(i), 2) + "\"},\n");
    }
    sbReadData.append("];\n");
    sbWriteData.append("];\n");

    return sbReadData.toString() + sbWriteData.toString() + sbNodesData.toString();
  }

  private float formatFloat(float f, int scale) {
    int scale_10 = (int) Math.pow(10, scale);
    return ((int) (f * scale_10)) * 1.0f / scale_10;
  }
}
