package tachyon.perf.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.task.RWTaskReport;
import tachyon.perf.task.TaskType;

public class WriteReport extends PerfReport {
  private String mFailedNodes;
  private int mFailedTasks;
  private long mId;
  private int mNodesNum;
  private String mWriteType;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;
  private List<Float[]> mWriteThroughput;

  protected WriteReport(TaskType taskType) {
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
    mWriteThroughput = new ArrayList<Float[]>(mNodesNum);

    for (File taskReportFile : taskReportFiles) {
      RWTaskReport taskReport = RWTaskReport.loadFromFile(taskReportFile);
      mNodes.add(taskReport.NODE_NAME);
      mWriteType = taskReport.getRWType();
      mAvaliableCores.add(taskReport.getCores());
      mWorkerMemory.add(taskReport.getTachyonWorkerBytes());
      if (taskReport.getStartTimeMs() < mId) {
        mId = taskReport.getStartTimeMs();
      }
      if (!taskReport.getSuccess()) {
        mFailedTasks ++;
        mFailedNodes = mFailedNodes + taskReport.NODE_NAME + " ";
        mWriteThroughput.add(new Float[0]);
        continue;
      }
      long[] bytes = taskReport.getSuccessBytes();
      long[] timeMs = taskReport.getThreadTimeMs();
      Float[] throughput = new Float[bytes.length];
      for (int i = 0; i < bytes.length; i ++) {
        // now throughput is in MB/s
        throughput[i] = bytes[i] / 1024.0f / 1024.0f / (timeMs[i] / 1000.0f);
      }
      mWriteThroughput.add(throughput);
    }
  }

  private String generateNodeDetails(int nodeIndex) {
    StringBuffer sbNodeDetail =
        new StringBuffer(mNodes.get(nodeIndex) + "'s throughput(MB/s) for each threads:\n\t");
    for (float throughput : mWriteThroughput.get(nodeIndex)) {
      sbNodeDetail.append("[ " + throughput + " ]");
    }
    sbNodeDetail.append("\n\n");
    return sbNodeDetail.toString();
  }

  private String generateWriteConf() {
    StringBuffer sbWriteConf = new StringBuffer();
    PerfConf perfConf = PerfConf.get();
    sbWriteConf.append("tachyon.perf.tfs.address\t" + perfConf.TFS_ADDRESS + "\n");
    sbWriteConf.append("tachyon.perf.write.file.length.bytes\t" + perfConf.FILE_LENGTH + "\n");
    sbWriteConf.append("tachyon.perf.write.files.per.thread\t" + perfConf.WRITE_FILES_PER_THREAD
        + "\n");
    sbWriteConf.append("tachyon.perf.write.threads.num\t" + perfConf.WRITE_THREADS_NUM + "\n");
    sbWriteConf.append("WRITE_TYPE\t" + mWriteType + "\n");
    return sbWriteConf.toString();
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
    StringBuffer sbThroughput = new StringBuffer("NodeName\tWriteThroughput(MB/s)\n");
    float totalThroughput = 0;
    for (int i = 0; i < mNodesNum; i ++) {
      float nodeThroughput = 0;
      for (float throughput : mWriteThroughput.get(i)) {
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
    fout.write("********** Write Test Settings **********\n");
    fout.write(generateWriteConf());
    fout.write("********** Write Throughput **********\n");
    fout.write(generateThroughput());
    fout.write("********** Node Details **********\n");
    for (int i = 0; i < mNodesNum; i ++) {
      fout.write(generateNodeDetails(i));
    }
    fout.close();
  }

}
