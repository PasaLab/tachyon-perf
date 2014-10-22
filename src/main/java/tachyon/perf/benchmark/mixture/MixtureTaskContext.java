package tachyon.perf.benchmark.mixture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.benchmark.SimpleTaskContext;

public class MixtureTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(3);
    List<Double> basicThroughputs = new ArrayList<Double>(threads.length);
    List<Double> readThroughputs = new ArrayList<Double>(threads.length);
    List<Double> writeThroughputs = new ArrayList<Double>(threads.length);
    for (PerfThread thread : threads) {
      if (!((MixtureThread) thread).getSuccess()) {
        mSuccess = false;
      }
      basicThroughputs.add(((MixtureThread) thread).getBasicWriteThroughput());
      readThroughputs.add(((MixtureThread) thread).getReadThroughput());
      writeThroughputs.add(((MixtureThread) thread).getWriteThroughput());
    }
    mAdditiveStatistics.put("BasicWriteThroughput(MB/s)", basicThroughputs);
    mAdditiveStatistics.put("ReadThroughput(MB/s)", readThroughputs);
    mAdditiveStatistics.put("WriteThroughput(MB/s)", writeThroughputs);
  }
}
