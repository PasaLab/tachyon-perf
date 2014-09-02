package tachyon.perf.benchmark;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.perf.benchmark.read.ReadMode;

/**
 * Unit tests for tachyon.perf.task.ListGenerator
 */
public class ListGeneratorTest {
  @Test
  public void generateReadFilesTest() {
    int threadsNum = 4;
    int filesPerThread = 6;
    List<Integer> candidates = new ArrayList<Integer>(20);
    for (int i = 0; i < 20; i++) {
      candidates.add(i);
    }

    List<Integer>[] result1 =
        ListGenerator.generateReadFiles(threadsNum, filesPerThread, candidates, ReadMode.RANDOM,
            true);
    Assert.assertEquals(threadsNum, result1.length);
    Assert.assertEquals(filesPerThread, result1[0].size());
    for (int i = 1; i < threadsNum; i++) {
      Assert.assertEquals(result1[0], result1[i]);
    }

    List<Integer>[] result2 =
        ListGenerator.generateReadFiles(threadsNum, filesPerThread, candidates, ReadMode.SEQUENCE,
            false);
    Assert.assertEquals(threadsNum, result2.length);
    for (int i = 0; i < threadsNum; i++) {
      Assert.assertEquals(filesPerThread, result2[i].size());
      for (int j = 0; j < filesPerThread; j++) {
        Integer expect = (20 / threadsNum * i + j) % 20;
        Assert.assertEquals(expect, result2[i].get(j));
      }
    }
  }

  @Test
  public void generateWriteFilesTest() {
    int threadsNum = 4;
    int filesPerThread = 6;
    String dirPrefix = "xyz";
    List<String>[] result = ListGenerator.generateWriteFiles(threadsNum, filesPerThread, dirPrefix);
    Assert.assertEquals(threadsNum, result.length);
    for (int i = 0; i < threadsNum; i++) {
      Assert.assertEquals(filesPerThread, result[i].size());
      for (int j = 0; j < filesPerThread; j++) {
        Assert.assertEquals("xyz/" + i + "-" + j, result[i].get(j));
      }
    }
  }
}
