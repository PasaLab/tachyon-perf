package tachyon.perf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.perf.PerfConstants
 */
public class PerfConstantsTest {
  @Test
  public void parseSizeByteTest() {
    long kb = 1024;
    long mb = 1024 * kb;
    long gb = 1024 * mb;
    long tb = 1024 * gb;
    long pb = 1024 * tb;
    long eb = 1024 * pb;
    String b_7 = PerfConstants.parseSizeByte(7);
    String kb_6 = PerfConstants.parseSizeByte(kb * 6);
    String mb_5 = PerfConstants.parseSizeByte(mb * 5);
    String gb_4 = PerfConstants.parseSizeByte(gb * 4);
    String tb_3 = PerfConstants.parseSizeByte(tb * 3);
    String pb_2 = PerfConstants.parseSizeByte(pb * 2);
    String eb_1 = PerfConstants.parseSizeByte(eb);
    Assert.assertEquals((float) 7 + "B", b_7);
    Assert.assertEquals((float) 6 + "KB", kb_6);
    Assert.assertEquals((float) 5 + "MB", mb_5);
    Assert.assertEquals((float) 4 + "GB", gb_4);
    Assert.assertEquals((float) 3 + "TB", tb_3);
    Assert.assertEquals((float) 2 + "PB", pb_2);
    Assert.assertEquals((float) 1 + "EB", eb_1);
  }
}
