package tachyon.perf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.perf.conf.PerfConf;

public class TachyonPerfSupervision {

  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.print("Wrong program arguments. Should be the number of test nodes");
      System.exit(-1);
    }

    try {
      int nodesNum = Integer.parseInt(args[0]);
      int remainNodesNum = nodesNum;
      boolean[] successFlags = new boolean[nodesNum];
      PerfConf perfConf = PerfConf.get();
      String tfsWorkDir = perfConf.TFS_ADDRESS + perfConf.TFS_DIR;
      TachyonFS tfs = TachyonFS.get(tfsWorkDir);

      while (remainNodesNum > 0) {
        System.out.println("TachyonPerf Running: remaining " + remainNodesNum + " nodes.");
        for (int i = 0; i < nodesNum; i ++) {
          if (!successFlags[i]) {
            String tfsSuccessPath = perfConf.TFS_DIR + "/" + i + "/SUCCESS";
            if (tfs.exist(tfsSuccessPath)) {
              tfs.delete(tfsSuccessPath, true);
              remainNodesNum --;
              successFlags[i] = true;
              System.out.println("\tTest Success: Node-" + i + ".");
            }
          }
        }
        Thread.sleep(2000);
      }

      System.out.println("All nodes succeed.");
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error when wait all nodes. Abort the test by "
          + "'/tachyon-perf-home/bin/tachyon-perf-abort'.");
    }

  }
}
