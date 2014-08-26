package tachyon.perf.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.perf.tools.Supervisible;

/**
 * Unit tests for tachyon.perf.task.PerfTask
 */
public class PerfTaskTest {
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
