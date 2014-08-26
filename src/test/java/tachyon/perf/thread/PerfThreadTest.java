package tachyon.perf.thread;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.thread.PerfThread
 */
public class PerfThreadTest {
  @Test
  public void readThreadConstructorTest() {
    ReadThread readThread = new ReadThread(0, null, null);
    Assert.assertEquals(0, readThread.ID);
  }

  @Test
  public void writeThreadConstructorTest() {
    WriteThread writeThread = new WriteThread(0, null, null);
    Assert.assertEquals(0, writeThread.ID);
  }
}
