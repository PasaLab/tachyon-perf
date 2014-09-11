package tachyon.perf.benchmark.createfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.client.WriteType;
import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.benchmark.write.WriteTaskContext;
import tachyon.perf.conf.PerfConf;

public class CreateFileTotalReport extends PerfTotalReport {
  private String mFailedSlaves = "";
  private int mFailedTasks = 0;
  private long mId = Long.MAX_VALUE;
  private int mSlavesNum;

  private List<String> mSlaves;

  private Map<String, Integer> mAvaliableCores;
  private Map<String, Long> mWorkerMemory;

  private List<Long> mElapsedTimeMs;
  private List<Integer[]> mSuccessFiles;
  private List<Long[]> mTimeStamps;

  @Override
  public void initialFromTaskContexts(File[] taskContextFiles) throws IOException {
    mSlavesNum = taskContextFiles.length;
    mSlaves = new ArrayList<String>(mSlavesNum);
    mAvaliableCores = new HashMap<String, Integer>();
    mWorkerMemory = new HashMap<String, Long>();
    mElapsedTimeMs = new ArrayList<Long>(mSlavesNum);
    mSuccessFiles = new ArrayList<Integer[]>(mSlavesNum);
    mTimeStamps = new ArrayList<Long[]>(mSlavesNum);

    for (File taskContextFile : taskContextFiles) {
      CreateFileTaskContext taskContext = CreateFileTaskContext.loadFromFile(taskContextFile);
      mSlaves.add(taskContext.getId() + "@" + taskContext.getNodeName());
      mAvaliableCores.put(taskContext.getNodeName(), taskContext.getCores());
      mWorkerMemory.put(taskContext.getNodeName(), taskContext.getTachyonWorkerBytes());
      if (taskContext.getStartTimeMs() < mId) {
        mId = taskContext.getStartTimeMs();
      }
      if (!taskContext.getSuccess()) {
        mFailedTasks++;
        mFailedSlaves += taskContext.getId() + "@" + taskContext.getNodeName() + " ";
        mElapsedTimeMs.add(0L);
        mSuccessFiles.add(new Integer[0]);
        mTimeStamps.add(new Long[0]);
        continue;
      }
      mElapsedTimeMs.add(taskContext.getFinishTimeMs() - taskContext.getStartTimeMs());
      List<Integer> slaveSuccessFiles = taskContext.getSuccessFiles();
      List<Long> slaveTimeStamps = taskContext.getTimeStamps();
      Integer[] files = new Integer[slaveSuccessFiles.size()];
      Long[] timeMs = new Long[slaveTimeStamps.size()];
      for (int i = 0; i < files.length; i++) {
        files[i] = slaveSuccessFiles.get(i);
        timeMs[i] = slaveTimeStamps.get(i);
      }
      mSuccessFiles.add(files);
      mTimeStamps.add(timeMs);
    }
  }

  private String generateSlaveDetails(int slaveIndex) {
    StringBuffer sbSlaveDetail =
        new StringBuffer(mSlaves.get(slaveIndex)
            + "'s performance(files/sec) of creating files: [total "
            + mElapsedTimeMs.get(slaveIndex) / 1000 + " seconds]\n");
    Integer[] files = mSuccessFiles.get(slaveIndex);
    Long[] timeMs = mTimeStamps.get(slaveIndex);
    sbSlaveDetail.append("\t" + new Date(timeMs[0]) + ":" + files[0] + " operations\n");
    for (int i = 1; i < files.length; i++) {
      double perf = (files[i] - files[i - 1]) * 1000.0 / (timeMs[i] - timeMs[i - 1]);
      sbSlaveDetail.append("\t" + new Date(timeMs[i]) + ":" + files[i] + " operations; "
          + ((int) (perf * 100)) / 100.0 + " files/sec\n");
    }
    sbSlaveDetail.append("\n");
    return sbSlaveDetail.toString();
  }

  private String generateCreateFileConf() {
    StringBuffer sbCreateFileConf = new StringBuffer();
    TaskConfiguration taskConf = TaskConfiguration.get("CreateFile", true);
    sbCreateFileConf.append("tachyon.perf.tfs.address\t" + PerfConf.get().TFS_ADDRESS + "\n");
    sbCreateFileConf.append("file.length.bytes\t" + taskConf.getProperty("file.length.bytes")
        + "\n");
    sbCreateFileConf.append("files.per.thread\t" + taskConf.getProperty("files.per.thread") + "\n");
    sbCreateFileConf.append("interval.seconds\t" + taskConf.getProperty("interval.seconds") + "\n");
    sbCreateFileConf.append("threads.num\t" + taskConf.getProperty("threads.num") + "\n");
    return sbCreateFileConf.toString();
  }

  private String generateSystemConf() {
    StringBuffer sbSystemConf = new StringBuffer("NodeName\tCores\tWorkerMemory\n");
    int totalCores = 0;
    long totalMemory = 0;
    for (Map.Entry<String, Integer> nodeCores : mAvaliableCores.entrySet()) {
      String node = nodeCores.getKey();
      int core = nodeCores.getValue();
      long memory = mWorkerMemory.get(node);
      totalCores += core;
      totalMemory += memory;
      sbSystemConf.append(node + "\t" + core + "\t" + PerfConstants.parseSizeByte(memory) + "\n");
    }
    sbSystemConf.append("Total\t" + totalCores + "\t" + PerfConstants.parseSizeByte(totalMemory)
        + "\n");
    return sbSystemConf.toString();
  }

  @Override
  public void writeToFile(String fileName) throws IOException {
    File outFile = new File(fileName);
    BufferedWriter fout = new BufferedWriter(new FileWriter(outFile));
    fout.write(mTaskType + " Test - ID : " + mId + "\n");
    if (mFailedTasks == 0) {
      fout.write("Finished Successfully\n");
    } else {
      fout.write("Failed: " + mFailedTasks + " slaves failed ( " + mFailedSlaves + ")\n");
    }
    fout.write("********** System Configuratiom **********\n");
    fout.write(generateSystemConf());
    fout.write("********** CreateFile Test Settings **********\n");
    fout.write(generateCreateFileConf());
    fout.write("********** Slave Details **********\n");
    for (int i = 0; i < mSlavesNum; i++) {
      fout.write(generateSlaveDetails(i));
    }
    fout.close();
  }

}
