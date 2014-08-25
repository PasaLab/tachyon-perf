package tachyon.perf.thread;

import java.io.IOException;
import java.util.List;

import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.conf.PerfTaskConf;

public class ReadThread extends PerfThread {
  private byte[] mContent;
  private List<Integer> mReadFileList;
  private int mReadGrainBytes;
  private ReadType mReadType;
  private String mTfsAddress;

  public ReadThread(int id, List<Integer> readFileList, ReadType readType) {
    super(id);
    mThreadReport = new RWThreadReport();

    mReadGrainBytes = PerfTaskConf.get().READ_GRAIN_BYTES;
    mContent = new byte[mReadGrainBytes];
    mReadFileList = readFileList;
    mReadType = readType;
    mTfsAddress = PerfConf.get().TFS_ADDRESS;
  }

  @Override
  public void run() {
    mThreadReport.setStartTimeMs(System.currentTimeMillis());
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Read Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    mThreadReport.setSuccess(true);
    for (int fileId : mReadFileList) {
      try {
        TachyonFile file = tachyonClient.getFile(fileId);
        InStream is = file.getInStream(mReadType);
        int readLen;
        while ((readLen = is.read(mContent)) > 0) {
          ((RWThreadReport) mThreadReport).addSuccessBytes(readLen);
        }
        is.close();
      } catch (IOException e) {
        LOG.error("Read thread " + ID + "failed to read file, FileId: " + fileId, e);
        mThreadReport.setSuccess(false);
        break;
      }
      ((RWThreadReport) mThreadReport).addSuccessFiles(1);
    }
    mThreadReport.setFinishTimeMs(System.currentTimeMillis());
  }
}
