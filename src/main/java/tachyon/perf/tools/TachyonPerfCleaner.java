package tachyon.perf.tools;

import java.io.IOException;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFileSystem;

public class TachyonPerfCleaner {
  public static void main(String[] args) {
    try {
      PerfFileSystem fs = PerfFileSystem.get(PerfConf.get().FS_ADDRESS);
      fs.delete(PerfConf.get().FS_WORK_DIR, true);
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed to clean workspace " + PerfConf.get().FS_WORK_DIR + " on "
          + PerfConf.get().FS_ADDRESS);
    }
    System.out.println("Clean the workspace " + PerfConf.get().FS_WORK_DIR + " on "
        + PerfConf.get().FS_ADDRESS);
  }
}
