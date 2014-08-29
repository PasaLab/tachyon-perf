package tachyon.perf.benchmark.read;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.benchmark.read.ReadThread
 */
public class ReadThreadTest {
  @Test
  public void readThreadConstructorTest() {
    ReadThread readThread = new ReadThread(0, null, null, 1);
    Assert.assertEquals(0, readThread.ID);
  }
}
