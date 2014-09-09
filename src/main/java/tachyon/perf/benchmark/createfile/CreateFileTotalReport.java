package tachyon.perf.benchmark.createfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import tachyon.client.WriteType;
import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.benchmark.write.WriteTaskContext;
import tachyon.perf.conf.PerfConf;

public class CreateFileTotalReport extends PerfTotalReport {
  private String mFailedNodes = "";
  private int mFailedTasks = 0;
  private long mId = Long.MAX_VALUE;
  private int mNodesNum;

  private List<String> mNodes;

  private List<Integer> mAvaliableCores;
  private List<Long> mWorkerMemory;

  private List<Long> mElapsedTimeMs;
  private List<Integer[]> mSuccessFiles;
  private List<Long[]> mTimeStamps;

  @Override
  public void initialFromTaskContexts(File[] taskContextFiles) throws IOException {
    mNodesNum = taskContextFiles.length;
    mNodes = new ArrayList<String>(mNodesNum);
    mAvaliableCores = new ArrayList<Integer>(mNodesNum);
    mWorkerMemory = new ArrayList<Long>(mNodesNum);
    mElapsedTimeMs = new ArrayList<Long>(mNodesNum);
    mSuccessFiles = new ArrayList<Integer[]>(mNodesNum);
    mTimeStamps = new ArrayList<Long[]>(mNodesNum);

    for (File taskContextFile : taskContextFiles) {
      CreateFileTaskContext taskContext = CreateFileTaskContext.loadFromFile(taskContextFile);
      mNodes.add(taskContext.getNodeName());
      mAvaliableCores.add(taskContext.getCores());
      mWorkerMemory.add(taskContext.getTachyonWorkerBytes());
      if (taskContext.getStartTimeMs() < mId) {
        mId = taskContext.getStartTimeMs();
      }
      if (!taskContext.getSuccess()) {
        mFailedTasks++;
        mFailedNodes = mFailedNodes + taskContext.getNodeName() + " ";
        mElapsedTimeMs.add(0L);
        mSuccessFiles.add(new Integer[0]);
        mTimeStamps.add(new Long[0]);
        continue;
      }
      mElapsedTimeMs.add(taskContext.getFinishTimeMs() - taskContext.getStartTimeMs());
      List<Integer> nodeSuccessFiles = taskContext.getSuccessFiles();
      List<Long> nodeTimeStamps = taskContext.getTimeStamps();
      Integer[] files = new Integer[nodeSuccessFiles.size()];
      Long[] timeMs = new Long[nodeTimeStamps.size()];
      for (int i = 0; i < files.length; i++) {
        files[i] = nodeSuccessFiles.get(i);
        timeMs[i] = nodeTimeStamps.get(i);
      }
      mSuccessFiles.add(files);
      mTimeStamps.add(timeMs);
    }
  }

  private String generateNodeDetails(int nodeIndex) {
    StringBuffer sbNodeDetail =
        new StringBuffer(mNodes.get(nodeIndex)
            + "'s performance(files/sec) of creating files: [total "
            + mElapsedTimeMs.get(nodeIndex) / 1000 + " seconds]\n");
    Integer[] files = mSuccessFiles.get(nodeIndex);
    Long[] timeMs = mTimeStamps.get(nodeIndex);
    sbNodeDetail.append("\t" + new Date(timeMs[0]) + ":" + files[0] + " operations\n");
    for (int i = 1; i < files.length; i++) {
      double perf = (files[i] - files[i - 1]) * 1000.0 / (timeMs[i] - timeMs[i - 1]);
      sbNodeDetail.append("\t" + new Date(timeMs[i]) + ":" + files[i] + " operations; "
          + ((int) (perf * 100)) / 100.0 + " files/sec\n");
    }
    sbNodeDetail.append("\n");
    return sbNodeDetail.toString();
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
    fout.write("********** CreateFile Test Settings **********\n");
    fout.write(generateCreateFileConf());
    fout.write("********** Node Details **********\n");
    for (int i = 0; i < mNodesNum; i++) {
      fout.write(generateNodeDetails(i));
    }
    fout.close();
  }

}
