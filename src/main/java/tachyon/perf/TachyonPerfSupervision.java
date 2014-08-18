package tachyon.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.perf.conf.PerfConf;

public class TachyonPerfSupervision {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  /**
   * @param args
   */
  public static void main(String[] args) {
    int nodesNum = 0;
    List<String> nodes = null;
    try {
      TestType testType = TestType.getTestType(args[0]);
      if (testType.isRead()) {
        ReadType.getOpType(args[1]);
      } else {
        WriteType.getOpType(args[1]);
      }
      nodesNum = Integer.parseInt(args[2]);
      nodes = new ArrayList<String>(nodesNum);
      for (int i = 0; i < nodesNum; i ++) {
        nodes.add(args[i + 3]);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Wrong arguments. See more in bin/tachyon-perf");
      LOG.error("Wrong arguments. See more in bin/tachyon-perf", e);
      System.exit(-1);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error when setup the TachyonPerfSupervision. Abort the test by "
          + "'bin/tachyon-perf-abort'.");
      LOG.error("Error when setup the TachyonPerfSupervision.", e);
      System.exit(-1);
    }

    try {
      int remainNodesNum = nodesNum;
      int successNodesNum = 0;
      int failedNodesNum = 0;
      int[] nodeStates = new int[nodesNum];
      PerfConf perfConf = PerfConf.get();
      String tfsWorkDir = perfConf.TFS_ADDRESS + perfConf.TFS_DIR;
      TachyonFS tfs = TachyonFS.get(tfsWorkDir);
      while (remainNodesNum > 0) {
        Thread.sleep(2000);
        for (int i = 0; i < nodesNum; i ++) {
          if (nodeStates[i] == 0) {
            String tfsSuccessPath = perfConf.TFS_DIR + "/" + i + "/SUCCESS";
            String tfsFailedPath = perfConf.TFS_DIR + "/" + i + "/FAILED";
            if (tfs.exist(tfsSuccessPath)) {
              tfs.delete(tfsSuccessPath, true);
              remainNodesNum --;
              successNodesNum ++;
              nodeStates[i] = 1;
              String successInfo = "Success: Node-" + i + "(" + nodes.get(i) + ")";
              System.out.println(" [ " + successInfo + " ]");
              LOG.info(successInfo);
            } else if (tfs.exist(tfsFailedPath)) {
              tfs.delete(tfsFailedPath, true);
              remainNodesNum --;
              failedNodesNum ++;
              nodeStates[i] = -1;
              String failedInfo = "Failed: Node-" + i + "(" + nodes.get(i) + ")";
              System.out.println(" [ " + failedInfo + " ]");
              LOG.info(failedInfo);
            }
          }
        }
        String status =
            "Remaining: " + remainNodesNum + " nodes. Finished: "
                + (successNodesNum + failedNodesNum) + " nodes. Failed: " + failedNodesNum
                + " nodes.";
        if (perfConf.STATUS_DEBUG) {
          StringBuffer sbRemainNodes = new StringBuffer("Remaining:");
          StringBuffer sbSuccessNodes = new StringBuffer("Success:");
          StringBuffer sbFailedNodes = new StringBuffer("Failed:");
          for (int i = 0; i < nodesNum; i ++) {
            if (nodeStates[i] == 0) {
              sbRemainNodes.append(" " + nodes.get(i));
            } else if (nodeStates[i] == 1) {
              sbSuccessNodes.append(" " + nodes.get(i));
            } else if (nodeStates[i] == -1) {
              sbFailedNodes.append(" " + nodes.get(i));
            }
          }
          status =
              status + "\n\t" + sbRemainNodes.toString() + "\n\t" + sbSuccessNodes.toString()
                  + "\n\t" + sbFailedNodes.toString();
        }
        System.out.println(status);
        LOG.info(status);
        System.out.println();
      }
      System.out.println("All nodes Finished.");
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error when wait all nodes. Abort the test by "
          + "'bin/tachyon-perf-abort'.");
      LOG.error("Error when wait all nodes.", e);
    }
  }
}
