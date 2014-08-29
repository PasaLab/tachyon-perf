package tachyon.perf.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.perf.FooTask;
import tachyon.perf.FooTaskContext;
import tachyon.perf.benchmark.read.ReadTask;
import tachyon.perf.benchmark.write.WriteTask;

/**
 * Unit tests for tachyon.perf.task.PerfTask
 */
public class PerfTaskTest {
  @Test
  public void fooTaskTest() throws IOException {
    PerfTask fooTask = new FooTask();
    TaskContext fooTaskContext = new FooTaskContext("test");
    Assert.assertFalse(fooTaskContext.mSuccess);
    Assert.assertEquals(0, ((FooTaskContext) fooTaskContext).getFoo());
    Assert.assertFalse(((FooTaskContext) fooTaskContext).getReady());
    Assert.assertFalse(((FooTaskContext) fooTaskContext).getWritten());
    Assert.assertTrue(fooTask.setup(fooTaskContext));
    Assert.assertTrue(((FooTaskContext) fooTaskContext).getReady());
    Assert.assertTrue(fooTask.run(fooTaskContext));
    Assert.assertEquals(5, ((FooTaskContext) fooTaskContext).getFoo());
    Assert.assertTrue(fooTask.cleanupTask(fooTaskContext));
    fooTaskContext.writeToFile("test");
    Assert.assertTrue(((FooTaskContext) fooTaskContext).getWritten());
  }

  @Test
  public void readTaskConstructorTest() throws IOException {
    List<String> args = new ArrayList<String>(1);
    args.add("CACHE");
    ReadTask readTask = (ReadTask) PerfTask.getPerfTask("test", 0, TaskType.Read, args);
    Assert.assertEquals(0, readTask.getId());
    Assert.assertEquals("test", readTask.getNodeName());
    Assert.assertEquals(TaskType.Read, readTask.getTaskType());
    Assert.assertTrue(readTask instanceof Supervisible);
  }

  @Test
  public void writeTaskConstructorTest() throws IOException {
    List<String> args = new ArrayList<String>(1);
    args.add("MUST_CACHE");
    WriteTask writeTask = (WriteTask) PerfTask.getPerfTask("test", 0, TaskType.Write, args);
    Assert.assertEquals(0, writeTask.getId());
    Assert.assertEquals("test", writeTask.getNodeName());
    Assert.assertEquals(TaskType.Write, writeTask.getTaskType());
    Assert.assertTrue(writeTask instanceof Supervisible);
  }
}
