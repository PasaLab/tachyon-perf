package tachyon.perf.benchmark.connect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.conf.PerfConf;

public class ConnectTotalReport extends PerfTotalReport {
  private String mFailedSlaves = "";
  private int mFailedTasks = 0;
  private long mId = Long.MAX_VALUE;
  private int mSlavesNum;

  private List<String> mSlaves;
  private Map<String, Integer> mAvaliableCores;
  private List<Float[]> mMetadataPerf;

  @Override
  public void initialFromTaskContexts(File[] taskContextFiles) throws IOException {
    mSlavesNum = taskContextFiles.length;
    mSlaves = new ArrayList<String>(mSlavesNum);
    mAvaliableCores = new HashMap<String, Integer>();
    mMetadataPerf = new ArrayList<Float[]>(mSlavesNum);

    for (File taskContextFile : taskContextFiles) {
      ConnectTaskContext taskContext = ConnectTaskContext.loadFromFile(taskContextFile);
      mSlaves.add(taskContext.getId() + "@" + taskContext.getNodeName());
      mAvaliableCores.put(taskContext.getNodeName(), taskContext.getCores());
      if (taskContext.getStartTimeMs() < mId) {
        mId = taskContext.getStartTimeMs();
      }
      if (!taskContext.getSuccess()) {
        mFailedTasks++;
        mFailedSlaves += taskContext.getId() + "@" + taskContext.getNodeName() + " ";
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

  private String generateSlaveDetails(int slaveIndex) {
    StringBuffer sbSlaveDetail =
        new StringBuffer(mSlaves.get(slaveIndex)
            + "'s metadata operation performance(ops/sec) for each threads:\n\t");
    for (float metadataPerf : mMetadataPerf.get(slaveIndex)) {
      sbSlaveDetail.append("[ " + metadataPerf + " ]");
    }
    sbSlaveDetail.append("\n\n");
    return sbSlaveDetail.toString();
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
    for (Map.Entry<String, Integer> nodeCores : mAvaliableCores.entrySet()) {
      String node = nodeCores.getKey();
      int core = nodeCores.getValue();
      totalCores += core;
      sbSystemConf.append(node + "\t" + core + "\n");
    }
    sbSystemConf.append("Total\t" + totalCores + "\n");
    return sbSystemConf.toString();
  }

  private String generateMetadataPerf() {
    StringBuffer sbThroughput =
        new StringBuffer("SlaveName\tMetadataOperationPerformance(ops/sec)\n");
    float totalPerf = 0;
    for (int i = 0; i < mSlavesNum; i++) {
      float slavePerf = 0;
      for (float metadataPerf : mMetadataPerf.get(i)) {
        slavePerf += metadataPerf;
      }
      totalPerf += slavePerf;
      sbThroughput.append(mSlaves.get(i) + "\t" + slavePerf + "\n");
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
      fout.write("Failed: " + mFailedTasks + " nodes failed ( " + mFailedSlaves + ")\n");
    }
    fout.write("********** System Configuratiom **********\n");
    fout.write(generateSystemConf());
    fout.write("********** Connect Test Settings **********\n");
    fout.write(generateConnectConf());
    fout.write("********** Metadata Operation Performace **********\n");
    fout.write(generateMetadataPerf());
    fout.write("********** Slave Details **********\n");
    for (int i = 0; i < mSlavesNum; i++) {
      fout.write(generateSlaveDetails(i));
    }
    fout.close();
  }
}
