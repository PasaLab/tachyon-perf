package tachyon.perf.benchmark.createfile;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

public class CreateFileThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  private static final int WRITE_GRAIN = 1024 * 1024;

  public final int ID;

  private byte[] mContent;
  private long mFileLength;
  private int mFileNum;
  private String mTfsAddress;
  private String mTargetDir;

  private int mSuccessFiles;

  public CreateFileThread(int id, int fileNum, long fileLength, String workDir) {
    ID = id;
    mFileLength = fileLength;
    mFileNum = fileNum;
    mTfsAddress = PerfConf.get().TFS_ADDRESS;
    mTargetDir = workDir;
    if (fileLength > 0) {
      mContent = new byte[WRITE_GRAIN];
    }
    mSuccessFiles = 0;
  }

  public synchronized int getSuccessFiles() {
    return mSuccessFiles;
  }

  @Override
  public void run() {
    try {
      TachyonFS tfs = TachyonFS.get(mTfsAddress);
      for (int i = 0; i < mFileNum; i++) {
        int fileId = tfs.createFile(mTargetDir + "/" + ID + "-" + i);
        if (mFileLength > 0) {
          TachyonFile file = tfs.getFile(fileId);
          OutStream os = file.getOutStream(WriteType.TRY_CACHE);
          long remainLength = mFileLength;
          while (remainLength >= WRITE_GRAIN) {
            os.write(mContent);
            remainLength -= WRITE_GRAIN;
          }
          if (remainLength > 0) {
            os.write(mContent, 0, (int) remainLength);
          }
          os.close();
        }
        synchronized (this) {
          mSuccessFiles = i;
        }
      }
      synchronized (this) {
        mSuccessFiles = mFileNum;
      }
    } catch (IOException e) {
      LOG.error("CreateFile Thread " + ID + " falied");
      throw new RuntimeException(e);
    }
  }
}
