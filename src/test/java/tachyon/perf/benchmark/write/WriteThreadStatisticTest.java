package tachyon.perf.benchmark.write;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.benchmark.write.WriteThreadStatistic
 */
public class WriteThreadStatisticTest {
  @Test
  public void setGetTest() {
    WriteThreadStatistic writeStatistics = new WriteThreadStatistic();
    writeStatistics.setStartTimeMs(9999);
    writeStatistics.setFinishTimeMs(99999);
    writeStatistics.setSuccess(true);
    writeStatistics.addSuccessBytes(999);
    writeStatistics.addSuccessFiles(99);
    Assert.assertEquals(9999, writeStatistics.getStartTimeMs());
    Assert.assertEquals(99999, writeStatistics.getFinishTimeMs());
    Assert.assertTrue(writeStatistics.getSuccess());
    Assert.assertEquals(999, writeStatistics.getSuccessBytes());
    Assert.assertEquals(99, writeStatistics.getSuccessFiles());
  }
}
