package tachyon.perf.benchmark.iterate;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;

public class IterateTask extends SimpleTask {
  @Override
  public String getCleanupDir() {
    return PerfConf.get().FS_WORK_DIR + "/iterate";
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workDir = PerfConf.get().FS_WORK_DIR + "/iterate";
    mTaskConf.addProperty("work.dir", workDir);
    LOG.info("Work dir " + workDir);
    return true;
  }
}
