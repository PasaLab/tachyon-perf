package tachyon.perf.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.ReadType;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.conf.PerfTaskConf;
import tachyon.perf.task.ReadTaskReport;
import tachyon.perf.task.TaskType;

public class ReadReport extends PerfReport {
  private String mFailedNodes;
  private int mFailedTasks;
  private long mId;
  private int mNodesNum;
  private ReadType mReadType;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;
  private List<Float[]> mReadThroughput;

  protected ReadReport(TaskType taskType) {
    super(taskType);
    mFailedNodes = "";
    mFailedTasks = 0;
    mId = Long.MAX_VALUE;
  }

  @Override
  public void initialFromTaskReports(File[] taskReportFiles) throws IOException {
    mNodesNum = taskReportFiles.length;
    mNodes = new ArrayList<String>(mNodesNum);
    mAvaliableCores = new ArrayList<Integer>(mNodesNum);
    mWorkerMemory = new ArrayList<Long>(mNodesNum);
    mReadThroughput = new ArrayList<Float[]>(mNodesNum);

    for (File taskReportFile : taskReportFiles) {
      ReadTaskReport taskReport = ReadTaskReport.loadFromFile(taskReportFile);
      mNodes.add(taskReport.NODE_NAME);
      mReadType = taskReport.getReadType();
      mAvaliableCores.add(taskReport.getCores());
      mWorkerMemory.add(taskReport.getTachyonWorkerBytes());
      if (taskReport.getStartTimeMs() < mId) {
        mId = taskReport.getStartTimeMs();
      }
      if (!taskReport.getSuccess()) {
        mFailedTasks ++;
        mFailedNodes = mFailedNodes + taskReport.NODE_NAME + " ";
        mReadThroughput.add(new Float[0]);
        continue;
      }
      long[] bytes = taskReport.getReadBytes();
      long[] timeMs = taskReport.getThreadTimeMs();
      Float[] throughput = new Float[bytes.length];
      for (int i = 0; i < bytes.length; i ++) {
        // now throughput is in MB/s
        throughput[i] = bytes[i] / 1024.0f / 1024.0f / (timeMs[i] / 1000.0f);
      }
      mReadThroughput.add(throughput);
    }
  }

  private String generateNodeDetails(int nodeIndex) {
    StringBuffer sbNodeDetail =
        new StringBuffer(mNodes.get(nodeIndex) + "'s throughput(MB/s) for each threads:\n\t");
    for (float throughput : mReadThroughput.get(nodeIndex)) {
      sbNodeDetail.append("[ " + throughput + " ]");
    }
    sbNodeDetail.append("\n\n");
    return sbNodeDetail.toString();
  }

  private String generateReadConf() {
    StringBuffer sbReadConf = new StringBuffer();
    PerfTaskConf perfTaskConf = PerfTaskConf.get();
    sbReadConf.append("tachyon.perf.tfs.address\t" + PerfConf.get().TFS_ADDRESS + "\n");
    sbReadConf.append("tachyon.perf.read.files.per.thread\t" + perfTaskConf.READ_FILES_PER_THREAD
        + "\n");
    sbReadConf.append("tachyon.perf.read.grain.bytes\t" + perfTaskConf.READ_GRAIN_BYTES + "\n");
    sbReadConf.append("tachyon.perf.read.identical\t" + perfTaskConf.READ_IDENTICAL + "\n");
    sbReadConf.append("tachyon.perf.read.mode\t" + perfTaskConf.READ_MODE + "\n");
    sbReadConf.append("tachyon.perf.read.threads.num\t" + perfTaskConf.READ_THREADS_NUM + "\n");
    sbReadConf.append("READ_TYPE\t" + mReadType.toString() + "\n");
    return sbReadConf.toString();
  }

  private String generateSystemConf() {
    StringBuffer sbSystemConf = new StringBuffer("NodeName\tCores\tWorkerMemory\n");
    int totalCores = 0;
    long totalMemory = 0;
    for (int i = 0; i < mNodesNum; i ++) {
      totalCores += mAvaliableCores.get(i);
      totalMemory += mWorkerMemory.get(i);
      sbSystemConf.append(mNodes.get(i) + "\t" + mAvaliableCores.get(i) + "\t"
          + PerfConstants.parseSizeByte(mWorkerMemory.get(i)) + "\n");
    }
    sbSystemConf.append("Total\t" + totalCores + "\t" + PerfConstants.parseSizeByte(totalMemory)
        + "\n");
    return sbSystemConf.toString();
  }

  private String generateThroughput() {
    StringBuffer sbThroughput = new StringBuffer("NodeName\tReadThroughput(MB/s)\n");
    float totalThroughput = 0;
    for (int i = 0; i < mNodesNum; i ++) {
      float nodeThroughput = 0;
      for (float throughput : mReadThroughput.get(i)) {
        nodeThroughput += throughput;
      }
      totalThroughput += nodeThroughput;
      sbThroughput.append(mNodes.get(i) + "\t" + nodeThroughput + "\n");
    }
    sbThroughput.append("Total\t" + totalThroughput + "\n");
    return sbThroughput.toString();
  }

  @Override
  public void writeToFile(String fileName) throws IOException {
    File outFile = new File(fileName);
    BufferedWriter fout = new BufferedWriter(new FileWriter(outFile));
    fout.write(TASK_TYPE.toString() + " Test - ID : " + mId + "\n");
    if (mFailedTasks == 0) {
      fout.write("Finished Successfully\n");
    } else {
      fout.write("Failed: " + mFailedTasks + " nodes failed ( " + mFailedNodes + ")\n");
    }
    fout.write("********** System Configuratiom **********\n");
    fout.write(generateSystemConf());
    fout.write("********** Read Test Settings **********\n");
    fout.write(generateReadConf());
    fout.write("********** Read Throughput **********\n");
    fout.write(generateThroughput());
    fout.write("********** Node Details **********\n");
    for (int i = 0; i < mNodesNum; i ++) {
      fout.write(generateNodeDetails(i));
    }
    fout.close();
  }
}
