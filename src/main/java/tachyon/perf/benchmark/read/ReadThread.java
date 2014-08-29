package tachyon.perf.benchmark.read;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

/**
 * Thread to read files from Tachyon.
 */
public class ReadThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public final int ID;

  private byte[] mContent;
  private List<Integer> mReadFileList;
  private int mReadGrainBytes;
  private ReadType mReadType;
  private ReadThreadStatistic mStatistic;
  private String mTfsAddress;

  public ReadThread(int id, List<Integer> readFileList, ReadType readType, int grainBytes) {
    ID = id;
    mReadGrainBytes = grainBytes;
    mContent = new byte[mReadGrainBytes];
    mReadFileList = readFileList;
    mReadType = readType;
    mStatistic = new ReadThreadStatistic();
    mTfsAddress = PerfConf.get().TFS_ADDRESS;
  }

  public ReadThreadStatistic getStatistic() {
    return mStatistic;
  }

  @Override
  public void run() {
    mStatistic.setStartTimeMs(System.currentTimeMillis());
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Read Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    mStatistic.setSuccess(true);
    for (int fileId : mReadFileList) {
      try {
        TachyonFile file = tachyonClient.getFile(fileId);
        InStream is = file.getInStream(mReadType);
        int readLen;
        while ((readLen = is.read(mContent)) > 0) {
          mStatistic.addSuccessBytes(readLen);
        }
        is.close();
      } catch (Exception e) {
        LOG.error("Read thread " + ID + "failed to read file, FileId: " + fileId, e);
        mStatistic.setSuccess(false);
        break;
      }
      mStatistic.addSuccessFiles(1);
    }
    mStatistic.setFinishTimeMs(System.currentTimeMillis());
  }
}
