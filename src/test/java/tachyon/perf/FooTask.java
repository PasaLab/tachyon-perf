package tachyon.perf;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.TaskContext;

/**
 * A simple task class just used for unit tests.
 */
public class FooTask extends PerfTask {

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    ((FooTaskContext) taskContext).setReady(true);
    return true;
  }

  @Override
  protected boolean runTask(TaskContext taskContext) {
    ((FooTaskContext) taskContext).setFoo(5);
    return true;
  }

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    taskContext.setSuccess(true);
    return true;
  }

}
