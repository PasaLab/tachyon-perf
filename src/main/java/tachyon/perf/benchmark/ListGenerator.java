package tachyon.perf.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Test file list generator
 */
public class ListGenerator {
  private static Random sRand = new Random(System.currentTimeMillis());

  public static List<String> generateRandomReadFiles(int filesNum, List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(sRand.nextInt(range)));
    }
    return ret;
  }

  public static List<String> generateSequenceReadFiles(int id, int threadsNum, int filesNum,
      List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    int index = range / threadsNum * id;
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(index));
      index = (index + 1) % range;
    }
    return ret;
  }

  public static List<String> generateWriteFiles(int id, int filesNum, String dirPrefix) {
    List<String> ret = new ArrayList<String>(filesNum);
    for (int i = 0; i < filesNum; i ++) {
      ret.add(dirPrefix + "/" + id + "-" + i);
    }
    return ret;
  }
}
