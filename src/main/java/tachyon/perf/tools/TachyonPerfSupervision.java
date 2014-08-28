package tachyon.perf.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskType;
import tachyon.perf.conf.PerfConf;

/**
 * Monitor the node states.
 */
public class TachyonPerfSupervision {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);
  private static final int NODE_STATE_FAILED = -1;
  private static final int NODE_STATE_INITIAL = 0;
  private static final int NODE_STATE_RUNNING = 1;
  private static final int NODE_STATE_SUCCESS = 2;

  private static int[] sNodeStates;
  private static PerfTask[] sNodeTasks;

  private static boolean allFinished() {
    int nodesNum = sNodeStates.length;
    int finishedNum = 0;
    for (int i = 0; i < nodesNum; i ++) {
      if (sNodeStates[i] == NODE_STATE_FAILED || sNodeStates[i] == NODE_STATE_SUCCESS) {
        finishedNum ++;
      }
    }
    return (finishedNum == nodesNum);
  }

  private static boolean needAbort(int round, int percentage) {
    int failedNodes = 0;
    for (int state : sNodeStates) {
      if ((state == NODE_STATE_FAILED) || (state == NODE_STATE_INITIAL && round > 30)) {
        failedNodes ++;
      }
    }
    int failedThreshold = percentage * sNodeStates.length / 100;
    return (failedNodes > failedThreshold);
  }

  private static void printNodeStatus(boolean debug, List<String> nodes) {
    int runningNodes = 0;
    int successNodes = 0;
    int failedNodes = 0;
    StringBuffer sbRunningNodes = null;
    StringBuffer sbSuccessNodes = null;
    StringBuffer sbFailedNodes = null;
    if (debug) {
      sbRunningNodes = new StringBuffer("Running:");
      sbSuccessNodes = new StringBuffer("Success:");
      sbFailedNodes = new StringBuffer("Failed:");
    }
    for (int i = 0; i < sNodeStates.length; i ++) {
      if (sNodeStates[i] == NODE_STATE_RUNNING) {
        runningNodes ++;
        if (debug) {
          sbRunningNodes.append(" " + nodes.get(i));
        }
      } else if (sNodeStates[i] == NODE_STATE_SUCCESS) {
        successNodes ++;
        if (debug) {
          sbSuccessNodes.append(" " + nodes.get(i));
        }
      } else if (sNodeStates[i] == NODE_STATE_FAILED) {
        failedNodes ++;
        if (debug) {
          sbFailedNodes.append(" " + nodes.get(i));
        }
      }
    }
    String status =
        "Running: " + runningNodes + " nodes. Success: " + successNodes + " nodes. Failed: "
            + failedNodes + " nodes.";
    if (debug) {
      status =
          status + "\n\t" + sbRunningNodes.toString() + "\n\t" + sbSuccessNodes.toString()
              + "\n\t" + sbFailedNodes.toString();
    }
    System.out.println(status);
    LOG.info(status);
    System.out.println();
  }

  public static void main(String[] args) {
    int nodesNum = 0;
    List<String> nodes = null;
    TaskType taskType = null;
    List<String> taskArgs = null;
    try {
      int index = 0;
      nodesNum = Integer.parseInt(args[0]);
      nodes = new ArrayList<String>(nodesNum);
      for (index = 1; index < nodesNum + 1; index ++) {
        nodes.add(args[index]);
      }
      taskType = TaskType.getTaskType(args[index]);
      taskArgs = new ArrayList<String>();
      index ++;
      for (int i = 0; i < args.length - index; i ++) {
        taskArgs.add(args[index + i]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Wrong arguments. Should be <NodesNum> [Nodes...] <TaskType> [args...]");
      LOG.error("Wrong arguments. Should be <NodesNum> [Nodes...] <TaskType> [args...]", e);
      System.exit(-1);
    }

    try {
      sNodeStates = new int[nodesNum];
      sNodeTasks = new PerfTask[nodesNum];
      for (int i = 0; i < nodesNum; i ++) {
        sNodeStates[i] = NODE_STATE_INITIAL;
        sNodeTasks[i] = PerfTask.getPerfTask(nodes.get(i), i, taskType, taskArgs);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Wrong arguments for task.");
      LOG.error("Wrong arguments for task.", e);
      System.exit(-1);
    }

    if (!(sNodeTasks[0] instanceof Supervisible)) {
      LOG.warn("TaskType " + taskType.toString() + " doesn't support Supervisible.");
      System.out.println("TaskType " + taskType.toString() + " doesn't support Supervisible.");
      System.out.println("TachyonPerfSupervision Exit.");
      System.exit(0);
    }

    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      int round = 0;
      while (!allFinished()) {
        Thread.sleep(2000);
        for (int i = 0; i < sNodeStates.length; i ++) {
          if (sNodeStates[i] == NODE_STATE_INITIAL) {
            String readyPath = ((Supervisible) sNodeTasks[i]).getTfsReadyPath();
            if (tfs.exist(readyPath)) {
              tfs.delete(readyPath, true);
              sNodeStates[i] = NODE_STATE_RUNNING;
              String runningInfo = "Node-" + i + "(" + nodes.get(i) + ") is running";
              System.out.println(" [ " + runningInfo + " ]");
              LOG.info(runningInfo);
            }
          } else if (sNodeStates[i] == NODE_STATE_RUNNING) {
            String failedPath = ((Supervisible) sNodeTasks[i]).getTfsFailedPath();
            String successPath = ((Supervisible) sNodeTasks[i]).getTfsSuccessPath();
            if (tfs.exist(failedPath)) {
              tfs.delete(failedPath, true);
              sNodeStates[i] = NODE_STATE_FAILED;
              String failedInfo = "Failed: Node-" + i + "(" + nodes.get(i) + ")";
              System.out.println(" [ " + failedInfo + " ]");
              LOG.info(failedInfo);
            } else if (tfs.exist(successPath)) {
              tfs.delete(successPath, true);
              sNodeStates[i] = NODE_STATE_SUCCESS;
              String successInfo = "Success: Node-" + i + "(" + nodes.get(i) + ")";
              System.out.println(" [ " + successInfo + " ]");
              LOG.info(successInfo);
            }
          }
        }
        round ++;
        printNodeStatus(perfConf.STATUS_DEBUG, nodes);
        if (perfConf.FAILED_THEN_ABORT && needAbort(round, perfConf.FAILED_PERCENTAGE)) {
          java.lang.Runtime.getRuntime().exec(
              perfConf.TACHYON_PERF_HOME + "/bin/tachyon-perf-abort");
          System.out.println("Enough nodes failed. Abort all the nodes.");
          LOG.error("Enough nodes failed. Abort all the nodes.");
        }
      }
      tfs.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Error when wait all nodes.");
      LOG.error("Error when wait all nodes.", e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.err.println("Error when thread sleep.");
      LOG.error("Error when thread sleep.", e);
    } catch (TException e) {
      e.printStackTrace();
      System.out.println("Failed to close the TachyonFS.");
      LOG.warn("Failed to close the TachyonFS.", e);
    }
    System.out.println("Tachyon-Perf End.");
  }
}
