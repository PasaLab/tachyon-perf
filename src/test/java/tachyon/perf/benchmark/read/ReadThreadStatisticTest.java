package tachyon.perf.benchmark.read;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.benchmark.read.ReadThreadStatistic
 */
public class ReadThreadStatisticTest {
  @Test
  public void setGetTest() {
    ReadThreadStatistic readStatistics = new ReadThreadStatistic();
    readStatistics.setStartTimeMs(9999);
    readStatistics.setFinishTimeMs(99999);
    readStatistics.setSuccess(true);
    readStatistics.addSuccessBytes(999);
    readStatistics.addSuccessFiles(99);
    Assert.assertEquals(9999, readStatistics.getStartTimeMs());
    Assert.assertEquals(99999, readStatistics.getFinishTimeMs());
    Assert.assertTrue(readStatistics.getSuccess());
    Assert.assertEquals(999, readStatistics.getSuccessBytes());
    Assert.assertEquals(99, readStatistics.getSuccessFiles());
  }
}
