package tachyon.perf.thread;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.thread.ThreadReport
 */
public class ThreadReportTest {
  @Test
  public void rwThreadReportTest() {
    RWThreadReport threadReport = new RWThreadReport();
    threadReport.setStartTimeMs(9999);
    threadReport.setFinishTimeMs(99999);
    threadReport.setSuccess(true);
    threadReport.addSuccessBytes(999);
    threadReport.addSuccessFiles(99);
    Assert.assertEquals(9999, threadReport.getStartTimeMs());
    Assert.assertEquals(99999, threadReport.getFinishTimeMs());
    Assert.assertTrue(threadReport.getSuccess());
    Assert.assertEquals(999, threadReport.getSuccessBytes());
    Assert.assertEquals(99, threadReport.getSuccessFiles());
  }
}
