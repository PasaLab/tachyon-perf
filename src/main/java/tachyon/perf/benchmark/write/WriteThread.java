package tachyon.perf.benchmark.write;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

/**
 * Thread to write files to Tachyon.
 */
public class WriteThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public final int ID;
  private byte[] mContent;
  private long mFileLength;
  private String mTfsAddress;
  private List<String> mWriteFileList;
  private int mWriteGrainBytes;
  private WriteThreadStatistic mStatistic;
  private WriteType mWriteType;

  public WriteThread(int id, List<String> writeFileList, WriteType writeType, long fileLength,
      int grainBytes) {
    ID = id;
    mWriteGrainBytes = grainBytes;
    mContent = new byte[mWriteGrainBytes];
    mFileLength = fileLength;
    mTfsAddress = PerfConf.get().TFS_ADDRESS;
    mWriteFileList = writeFileList;
    mStatistic = new WriteThreadStatistic();
    mWriteType = writeType;
    // TODO: content initialize?
  }

  public WriteThreadStatistic getStatistic() {
    return mStatistic;
  }

  @Override
  public void run() {
    mStatistic.setStartTimeMs(System.currentTimeMillis());
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Write Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    mStatistic.setSuccess(true);
    for (String fileName : mWriteFileList) {
      try {
        int fileId = -1;
        fileId = tachyonClient.createFile(fileName);
        if (fileId == -1) {
          throw new IOException("Failed to create file " + fileName);
        }

        TachyonFile file = tachyonClient.getFile(fileId);
        OutStream os = file.getOutStream(mWriteType);
        long remainLength = mFileLength;
        while (remainLength >= mWriteGrainBytes) {
          os.write(mContent);
          mStatistic.addSuccessBytes(mWriteGrainBytes);
          remainLength -= mWriteGrainBytes;
        }
        if (remainLength > 0) {
          os.write(mContent, 0, (int) remainLength);
          mStatistic.addSuccessBytes(remainLength);
        }
        os.close();
      } catch (Exception e) {
        LOG.error("Write thread " + ID + "failed to write file " + fileName, e);
        mStatistic.setSuccess(false);
        break;
      }
      mStatistic.addSuccessFiles(1);
    }
    mStatistic.setFinishTimeMs(System.currentTimeMillis());
  }
}
