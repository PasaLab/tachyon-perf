package tachyon.perf.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import tachyon.perf.benchmark.read.ReadMode;

/**
 * Test file list generator
 */
public class ListGenerator {
  private static Random sRand = new Random(System.currentTimeMillis());

  private static List<Integer> generateSingleRandomReadFiles(int filesPerThread,
      List<Integer> candidates) {
    List<Integer> ret = new ArrayList<Integer>(filesPerThread);
    int range = candidates.size();
    for (int i = 0; i < filesPerThread; i++) {
      ret.add(candidates.get(sRand.nextInt(range)));
    }
    return ret;
  }

  private static List<Integer> generateSingleSequenceReadFiles(int id, int threadsNum,
      int filesPerThread, List<Integer> candidates) {
    List<Integer> ret = new ArrayList<Integer>(filesPerThread);
    int range = candidates.size();
    int index = range / threadsNum * id;
    for (int i = 0; i < filesPerThread; i++) {
      ret.add(candidates.get(index));
      index = (index + 1) % range;
    }
    return ret;
  }

  /**
   * Generate read file lists for read test
   * 
   * @param threadsNum number of the read threads
   * @param filesPerThread number of the read files of each read thread
   * @param candidates all the read file candidates' id
   * @param readMode read mode of the read test
   * @param identical true means all the read threads use the same list
   * @return the read list array. Each list is for a read thread, contains read files' id
   */
  public static List<Integer>[] generateReadFiles(int threadsNum, int filesPerThread,
      List<Integer> candidates, ReadMode readMode, boolean identical) {
    List<Integer>[] ret = new List[threadsNum];
    if (identical) {
      if (readMode.isRandom()) {
        ret[0] = generateSingleRandomReadFiles(filesPerThread, candidates);
      } else if (readMode.isSequence()) {
        ret[0] = generateSingleSequenceReadFiles(0, threadsNum, filesPerThread, candidates);
      }
      for (int i = 1; i < threadsNum; i++) {
        ret[i] = new ArrayList<Integer>(ret[0]);
      }
    } else {
      for (int i = 0; i < threadsNum; i++) {
        if (readMode.isRandom()) {
          ret[i] = generateSingleRandomReadFiles(filesPerThread, candidates);
        } else if (readMode.isSequence()) {
          ret[i] = generateSingleSequenceReadFiles(i, threadsNum, filesPerThread, candidates);
        }
      }
    }
    return ret;
  }

  /**
   * Generate write file lists for write test
   * 
   * @param threadsNum number of the write threads
   * @param filesPerThread number of the write files of each write thread
   * @param dirPrefix the workspace path of the write test in Tachyon File System
   * @return the write list array. Each list is for a write thread, contains write files' path
   */
  public static List<String>[] generateWriteFiles(int threadsNum, int filesPerThread,
      String dirPrefix) {
    List<String>[] ret = new List[threadsNum];
    for (int i = 0; i < threadsNum; i++) {
      ret[i] = new ArrayList<String>(filesPerThread);
      for (int j = 0; j < filesPerThread; j++) {
        ret[i].add(dirPrefix + "/" + i + "-" + j);
      }
    }
    return ret;
  }
}
