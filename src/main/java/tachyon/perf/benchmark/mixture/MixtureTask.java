package tachyon.perf.benchmark.mixture;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;

public class MixtureTask extends SimpleTask {
  @Override
  public String getCleanupDir() {
    return PerfConf.get().FS_WORK_DIR + "/mixture";
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workDir = PerfConf.get().FS_WORK_DIR + "/mixture/" + mId;
    mTaskConf.addProperty("work.dir", workDir);
    LOG.info("Work dir " + workDir);
    return true;
  }
}
