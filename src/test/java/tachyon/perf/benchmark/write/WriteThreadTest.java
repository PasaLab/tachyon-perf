package tachyon.perf.benchmark.write;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.benchmark.write.WriteThread
 */
public class WriteThreadTest {
  @Test
  public void readThreadConstructorTest() {
    WriteThread readThread = new WriteThread(0, null, null, 1, 1);
    Assert.assertEquals(0, readThread.ID);
  }
}
