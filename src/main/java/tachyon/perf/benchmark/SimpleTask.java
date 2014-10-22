package tachyon.perf.benchmark;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.conf.PerfConf;

public class SimpleTask extends PerfTask {
  @Override
  public String getCleanupDir() {
    return null;
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workspacePath = PerfConf.get().FS_WORK_DIR;
    LOG.info("FS workspace " + workspacePath);
    return true;
  }

  @Override
  protected boolean cleanupTask(PerfTaskContext taskContext) {
    return true;
  }
}
