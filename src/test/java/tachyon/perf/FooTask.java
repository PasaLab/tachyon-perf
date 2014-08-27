package tachyon.perf;

import tachyon.perf.task.PerfTask;
import tachyon.perf.task.TaskReport;

/**
 * A simple task class just used for unit tests.
 */
public class FooTask extends PerfTask {

  @Override
  protected boolean setupTask(TaskReport taskReport) {
    ((FooTaskReport) taskReport).setReady(true);
    return true;
  }

  @Override
  protected boolean runTask(TaskReport taskReport) {
    ((FooTaskReport) taskReport).setFoo(5);
    return true;
  }

  @Override
  protected boolean cleanupTask(TaskReport taskReport) {
    taskReport.setSuccess(true);
    return true;
  }

}
