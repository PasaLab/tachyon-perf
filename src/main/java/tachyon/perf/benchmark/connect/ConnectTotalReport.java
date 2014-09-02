package tachyon.perf.benchmark.connect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.conf.PerfConf;

public class ConnectTotalReport extends PerfTotalReport {
  private String mFailedNodes = "";
  private int mFailedTasks = 0;
  private long mId = Long.MAX_VALUE;
  private int mNodesNum;

  private List<String> mNodes;
  private List<Integer> mAvaliableCores;
  private List<Float[]> mMetadataPerf;

  @Override
  public void initialFromTaskContexts(File[] taskContextFiles) throws IOException {
    mNodesNum = taskContextFiles.length;
    mNodes = new ArrayList<String>(mNodesNum);
    mAvaliableCores = new ArrayList<Integer>(mNodesNum);
    mMetadataPerf = new ArrayList<Float[]>(mNodesNum);

    for (File taskContextFile : taskContextFiles) {
      ConnectTaskContext taskContext = ConnectTaskContext.loadFromFile(taskContextFile);
      mNodes.add(taskContext.getNodeName());
      mAvaliableCores.add(taskContext.getCores());
      if (taskContext.getStartTimeMs() < mId) {
        mId = taskContext.getStartTimeMs();
      }
      if (!taskContext.getSuccess()) {
        mFailedTasks++;
        mFailedNodes = mFailedNodes + taskContext.getNodeName() + " ";
        mMetadataPerf.add(new Float[0]);
        continue;
      }
      int[] ops = taskContext.getOps();
      long[] timeMs = taskContext.getThreadTimeMs();
      Float[] metadataPerf = new Float[ops.length];
      for (int i = 0; i < ops.length; i++) {
        // now it's in ops/sec
        metadataPerf[i] = ops[i] / (timeMs[i] / 1000.0f);
      }
      mMetadataPerf.add(metadataPerf);
    }
  }

  private String generateNodeDetails(int nodeIndex) {
    StringBuffer sbNodeDetail =
        new StringBuffer(mNodes.get(nodeIndex)
            + "'s metadata operation performance(ops/sec) for each threads:\n\t");
    for (float metadataPerf : mMetadataPerf.get(nodeIndex)) {
      sbNodeDetail.append("[ " + metadataPerf + " ]");
    }
    sbNodeDetail.append("\n\n");
    return sbNodeDetail.toString();
  }

  private String generateConnectConf() {
    StringBuffer sbConnectConf = new StringBuffer();
    TaskConfiguration taskConf = TaskConfiguration.get("Connect", true);
    sbConnectConf.append("tachyon.perf.tfs.address\t" + PerfConf.get().TFS_ADDRESS + "\n");
    sbConnectConf
        .append("clients.per.thread\t" + taskConf.getProperty("clients.per.thread") + "\n");
    sbConnectConf.append("ops.per.thread\t" + taskConf.getProperty("ops.per.thread") + "\n");
    sbConnectConf.append("threads.num\t" + taskConf.getProperty("threads.num") + "\n");
    return sbConnectConf.toString();
  }

  private String generateSystemConf() {
    StringBuffer sbSystemConf = new StringBuffer("NodeName\tCores\n");
    int totalCores = 0;
    for (int i = 0; i < mNodesNum; i++) {
      totalCores += mAvaliableCores.get(i);
      sbSystemConf.append(mNodes.get(i) + "\t" + mAvaliableCores.get(i) + "\n");
    }
    sbSystemConf.append("Total\t" + totalCores + "\n");
    return sbSystemConf.toString();
  }

  private String generateMetadataPerf() {
    StringBuffer sbThroughput =
        new StringBuffer("NodeName\tMetadataOperationPerformance(ops/sec)\n");
    float totalPerf = 0;
    for (int i = 0; i < mNodesNum; i++) {
      float nodePerf = 0;
      for (float metadataPerf : mMetadataPerf.get(i)) {
        nodePerf += metadataPerf;
      }
      totalPerf += nodePerf;
      sbThroughput.append(mNodes.get(i) + "\t" + nodePerf + "\n");
    }
    sbThroughput.append("Total\t" + totalPerf + "\n");
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
    fout.write("********** Connect Test Settings **********\n");
    fout.write(generateConnectConf());
    fout.write("********** Metadata Operation Performace **********\n");
    fout.write(generateMetadataPerf());
    fout.write("********** Node Details **********\n");
    for (int i = 0; i < mNodesNum; i++) {
      fout.write(generateNodeDetails(i));
    }
    fout.close();
  }
}
