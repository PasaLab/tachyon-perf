package tachyon.perf.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.client.ReadType;
import tachyon.client.WriteType;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.conf.PerfTaskConf;
import tachyon.perf.task.ReadTaskReport;
import tachyon.perf.task.TaskType;
import tachyon.perf.task.WriteTaskReport;

/**
 * This class is used to generate an html report. The prerequisite is that both write and read tests
 * are finished.
 */
public class TachyonPerfHtmlReport {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Wrong program arguments.");
      System.exit(-1);
    }

    TachyonPerfHtmlReport perfCollector = new TachyonPerfHtmlReport(args[0], args[1]);
    perfCollector.generateHtmlReport();
  }

  private final String REPORT_DIR;
  private final String WEB_FRAME_FILE_NAME;

  private StringBuffer mHtmlContent;
  private int mReadFailed = 0;
  private long mReadStartTimeMs = Long.MAX_VALUE;
  private ReadType mReadType;
  private int mWriteFailed = 0;
  private long mWriteStartTimeMs = Long.MAX_VALUE;
  private WriteType mWriteType;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;
  private List<Float[]> mReadThroughput;
  private List<Float[]> mWriteThroughput;

  public TachyonPerfHtmlReport(String reportDir, String webFrameFileName) {
    REPORT_DIR = reportDir;
    WEB_FRAME_FILE_NAME = webFrameFileName;
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
    File htmlFrameFile = new File(WEB_FRAME_FILE_NAME);
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

    File reportFileDir = new File(REPORT_DIR + "/node-reports");
    if (!reportFileDir.isDirectory()) {
      System.err.println("Failed to collect data. Make sure run both write and read tests.");
      return false;
    }
    File[] reportFiles = reportFileDir.listFiles();
    Set<String> nodes = new HashSet<String>();
    for (File reportFile : reportFiles) {
      String[] parts = reportFile.getName().split("-");
      nodes.add(parts[parts.length - 1]);
    }
    if (nodes.size() == 0) {
      System.err.println("Failed to collect data. Make sure run both write and read tests.");
      return false;
    }

    try {
      for (String node : nodes) {
        File readReportFile =
            new File(REPORT_DIR + "/node-reports/" + PerfConstants.PERF_REPORT_FILE_NAME_PREFIX
                + "-" + TaskType.Read.toString() + "-" + node);
        File writeReportFile =
            new File(REPORT_DIR + "/node-reports/" + PerfConstants.PERF_REPORT_FILE_NAME_PREFIX
                + "-" + TaskType.Write.toString() + "-" + node);
        mNodes.add(node);
        loadSingleReadTaskReport(readReportFile);
        loadSingleWriteTaskReport(writeReportFile);
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  private void loadSingleReadTaskReport(File readTaskReportFile) throws IOException {
    ReadTaskReport readTaskReport = ReadTaskReport.loadFromFile(readTaskReportFile);
    mReadType = readTaskReport.getReadType();
    if (readTaskReport.getStartTimeMs() < mReadStartTimeMs) {
      mReadStartTimeMs = readTaskReport.getStartTimeMs();
    }
    if (!readTaskReport.getSuccess()) {
      mReadFailed ++;
      return;
    }
    long[] bytes = readTaskReport.getReadBytes();
    long[] timeMs = readTaskReport.getThreadTimeMs();
    Float[] throughput = new Float[bytes.length];
    for (int i = 0; i < bytes.length; i ++) {
      // now throughput is in MB/s
      throughput[i] = bytes[i] / 1024.0f / 1024.0f / (timeMs[i] / 1000.0f);
    }
    mReadThroughput.add(throughput);
  }

  private void loadSingleWriteTaskReport(File writeTaskReportFile) throws IOException {
    WriteTaskReport writeTaskReport = WriteTaskReport.loadFromFile(writeTaskReportFile);
    mAvaliableCores.add(writeTaskReport.getCores());
    mWorkerMemory.add(writeTaskReport.getTachyonWorkerBytes());
    mWriteType = writeTaskReport.getWriteType();
    if (writeTaskReport.getStartTimeMs() < mWriteStartTimeMs) {
      mWriteStartTimeMs = writeTaskReport.getStartTimeMs();
    }
    if (!writeTaskReport.getSuccess()) {
      mWriteFailed ++;
      return;
    }
    long[] bytes = writeTaskReport.getWriteBytes();
    long[] timeMs = writeTaskReport.getThreadTimeMs();
    Float[] throughput = new Float[bytes.length];
    for (int i = 0; i < bytes.length; i ++) {
      // now throughput is in MB/s
      throughput[i] = bytes[i] / 1024.0f / 1024.0f / (timeMs[i] / 1000.0f);
    }
    mWriteThroughput.add(throughput);
  }

  public void generateHtmlReport() {
    boolean collectSuccess = collectData();
    String totalState = "";
    String nodesInfo = "";
    String perfConf = "";
    String nodesThroughput = "";
    String throughputData = "";
    if (!collectSuccess) {
      totalState = "\n<h2>Tachyon-Perf. Error when collect test results.</h2>\n";
    } else if ((mReadFailed + mWriteFailed) != 0) {
      totalState =
          "\n<h2>Tachyon-Perf Job ID : " + mWriteStartTimeMs + "|" + mReadStartTimeMs
              + "</h2>\n<h3>Job Status: Failed (" + mWriteFailed + " Write Tests Failed and "
              + mReadFailed + " Read Tests Failed)</h3>\n";
    } else {
      totalState =
          "\n<h2>Tachyon-Perf Job ID : " + mWriteStartTimeMs + "|" + mReadStartTimeMs
              + "</h2>\n<h3>Job Status: Finished Successfully</h3>\n";
      nodesInfo = generateNodesInfo();
      perfConf = generatePerfConf();
      nodesThroughput = generateNodesThroughput();
      throughputData = generateThroughputData();
    }
    File htmlReportFile = new File(REPORT_DIR + "/report.html");
    try {
      FileOutputStream htmlOutput = new FileOutputStream(htmlReportFile);
      String finalReport =
          mHtmlContent.toString().replace("$$TOTAL_STATE", totalState)
              .replace("$$NODES_INFO", nodesInfo).replace("$$PERF_CONF", perfConf)
              .replace("$$NODES_THROUGHPUT", nodesThroughput)
              .replace("$$THROUGHPUT_DATA", throughputData);
      htmlOutput.write(finalReport.getBytes());
      htmlOutput.close();
      System.out.println("Html Report generated at " + REPORT_DIR + "/report.html");
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
    PerfTaskConf perfTaskConf = PerfTaskConf.get();
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.tfs.address" + "</td>\n\t<td>"
        + PerfConf.get().TFS_ADDRESS + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.files.per.thread" + "</td>\n\t<td>"
        + perfTaskConf.READ_FILES_PER_THREAD + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.grain.bytes" + "</td>\n\t<td>"
        + perfTaskConf.READ_GRAIN_BYTES + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.identical" + "</td>\n\t<td>"
        + perfTaskConf.READ_IDENTICAL + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.mode" + "</td>\n\t<td>"
        + perfTaskConf.READ_MODE + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.read.threads.num" + "</td>\n\t<td>"
        + perfTaskConf.READ_THREADS_NUM + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.file.length.bytes" + "</td>\n\t<td>"
        + perfTaskConf.WRITE_FILE_LENGTH + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.files.per.thread" + "</td>\n\t<td>"
        + perfTaskConf.WRITE_FILES_PER_THREAD + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.grain.bytes" + "</td>\n\t<td>"
        + perfTaskConf.WRITE_GRAIN_BYTES + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "tachyon.perf.write.threads.num" + "</td>\n\t<td>"
        + perfTaskConf.WRITE_THREADS_NUM + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "READ_TYPE" + "</td>\n\t<td>" + mReadType.toString()
        + "</td>\n</tr>\n");
    sbPerfConf.append("<tr>\n\t<td>" + "WRITE_TYPE" + "</td>\n\t<td>" + mWriteType.toString()
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
