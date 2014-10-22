package tachyon.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFileSystem;
import tachyon.perf.thrift.SlaveAlreadyRegisterException;
import tachyon.perf.thrift.SlaveNotRegisterException;

public class SlaveStatus {
  private final int mSlavesNum;

  private final List<String> mCleanupDirs;
  private final Set<String> mFailedSlaves;
  private final Set<String> mSuccessSlaves;
  private final Set<String> mReadySlaves;
  private final Set<String> mUnregisterSlaves;

  public SlaveStatus(int slavesNum, Set<String> allSlaves) {
    mSlavesNum = slavesNum;
    mUnregisterSlaves = allSlaves;
    mCleanupDirs = new ArrayList<String>(mSlavesNum);
    mFailedSlaves = new HashSet<String>();
    mSuccessSlaves = new HashSet<String>();
    mReadySlaves = new HashSet<String>();
  }

  public synchronized boolean allReady(String slaveName) throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    return (mReadySlaves.size() == mSlavesNum);
  }

  public synchronized void slaveFinish(String slaveName, boolean success)
      throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    if (success && !mFailedSlaves.contains(slaveName)) {
      mSuccessSlaves.add(slaveName);
    } else {
      mFailedSlaves.add(slaveName);
    }
  }

  public synchronized void slaveReady(String slaveName, boolean success)
      throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    mReadySlaves.add(slaveName);
    if (!success) {
      mFailedSlaves.add(slaveName);
    }
  }

  public synchronized boolean slaveRegister(String slaveName, String cleanupDir)
      throws SlaveAlreadyRegisterException {
    if (mReadySlaves.contains(slaveName)) {
      throw new SlaveAlreadyRegisterException(slaveName + " already register");
    }
    mUnregisterSlaves.remove(slaveName);
    mReadySlaves.add(slaveName);
    if (cleanupDir != null) {
      mCleanupDirs.add(cleanupDir);
    }
    return true;
  }

  public void cleanup() throws IOException {
    PerfFileSystem fs = PerfFileSystem.get(PerfConf.get().FS_ADDRESS);
    synchronized (this) {
      for (String dir : mCleanupDirs) {
        if (fs.exists(dir)) {
          fs.delete(dir, true);
        }
      }
    }
    fs.close();
  }

  public synchronized int finished(boolean failedThenAbort, int failedPercentage) {
    int success, failed;
    failed = mFailedSlaves.size();
    success = mSuccessSlaves.size();
    if (failedThenAbort && (failed > (failedPercentage * mSlavesNum / 100))) {
      return -1;
    }
    if (success + failed == mSlavesNum) {
      return 1;
    }
    return 0;
  }

  public synchronized String getFinishStatus(boolean debug) {
    StringBuffer sbStatus = new StringBuffer();
    int running, success, failed;
    failed = mFailedSlaves.size();
    success = mSuccessSlaves.size();
    running = mReadySlaves.size() - failed - success;
    sbStatus.append("Running: ").append(running).append(" slaves. Success: ").append(success)
        .append(" slaves. Failed: ").append(failed).append(" slaves.");
    if (debug) {
      StringBuffer sbRunningSlaves = new StringBuffer("Running:");
      StringBuffer sbSuccessSlaves = new StringBuffer("Success:");
      StringBuffer sbFailedSlaves = new StringBuffer("Failed:");
      for (String slave : mReadySlaves) {
        if (mFailedSlaves.contains(slave)) {
          sbFailedSlaves.append(" " + slave);
        } else if (mSuccessSlaves.contains(slave)) {
          sbSuccessSlaves.append(" " + slave);
        } else {
          sbRunningSlaves.append(" " + slave);
        }
      }
      sbStatus.append("\n\t").append(sbRunningSlaves).append("\n\t").append(sbSuccessSlaves)
          .append("\n\t").append(sbFailedSlaves).append("\n");
    }
    return sbStatus.toString();
  }

  public synchronized boolean allRegistered() {
    return mUnregisterSlaves.isEmpty();
  }

  public synchronized List<String> getUnregisterSlaves() {
    return new ArrayList<String>(mUnregisterSlaves);
  }
}
