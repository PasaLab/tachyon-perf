package tachyon.perf.benchmark.read;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.ReadType;
import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.conf.PerfConf;

/**
 * Total report for read test.
 */
public class ReadTotalReport extends PerfTotalReport {
  private String mFailedNodes = "";
  private int mFailedTasks = 0;
  private long mId = Long.MAX_VALUE;
  private int mNodesNum;
  private ReadType mReadType;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;
  private List<Float[]> mReadThroughput;

  @Override
  public void initialFromTaskContexts(File[] taskContextFiles) throws IOException {
    mNodesNum = taskContextFiles.length;
    mNodes = new ArrayList<String>(mNodesNum);
    mAvaliableCores = new ArrayList<Integer>(mNodesNum);
    mWorkerMemory = new ArrayList<Long>(mNodesNum);
    mReadThroughput = new ArrayList<Float[]>(mNodesNum);

    for (File taskContextFile : taskContextFiles) {
      ReadTaskContext taskContext = ReadTaskContext.loadFromFile(taskContextFile);
      mNodes.add(taskContext.getNodeName());
      mReadType = taskContext.getReadType();
      mAvaliableCores.add(taskContext.getCores());
      mWorkerMemory.add(taskContext.getTachyonWorkerBytes());
      if (taskContext.getStartTimeMs() < mId) {
        mId = taskContext.getStartTimeMs();
      }
      if (!taskContext.getSuccess()) {
        mFailedTasks++;
        mFailedNodes = mFailedNodes + taskContext.getNodeName() + " ";
        mReadThroughput.add(new Float[0]);
        continue;
      }
      long[] bytes = taskContext.getReadBytes();
      long[] timeMs = taskContext.getThreadTimeMs();
      Float[] throughput = new Float[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
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
    TaskConfiguration taskConf = TaskConfiguration.get("Read", true);
    sbReadConf.append("tachyon.perf.tfs.address\t" + PerfConf.get().TFS_ADDRESS + "\n");
    sbReadConf.append("files.per.thread\t" + taskConf.getProperty("files.per.thread") + "\n");
    sbReadConf.append("grain.bytes\t" + taskConf.getProperty("grain.bytes") + "\n");
    sbReadConf.append("identical\t" + taskConf.getProperty("identical") + "\n");
    sbReadConf.append("mode\t" + taskConf.getProperty("mode") + "\n");
    sbReadConf.append("threads.num\t" + taskConf.getProperty("threads.num") + "\n");
    sbReadConf.append("READ_TYPE\t" + mReadType.toString() + "\n");
    return sbReadConf.toString();
  }

  private String generateSystemConf() {
    StringBuffer sbSystemConf = new StringBuffer("NodeName\tCores\tWorkerMemory\n");
    int totalCores = 0;
    long totalMemory = 0;
    for (int i = 0; i < mNodesNum; i++) {
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
    for (int i = 0; i < mNodesNum; i++) {
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
    fout.write(mTaskType + " Test - ID : " + mId + "\n");
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
    for (int i = 0; i < mNodesNum; i++) {
      fout.write(generateNodeDetails(i));
    }
    fout.close();
  }
}
