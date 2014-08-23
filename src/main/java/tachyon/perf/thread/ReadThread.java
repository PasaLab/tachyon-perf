package tachyon.perf.thread;

import java.io.IOException;
import java.util.List;

import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.perf.conf.PerfConf;

public class ReadThread extends PerfThread {
  private static final int DEFALUT_READ_GRAIN = 4194304;// 4MB

  private byte[] mContent;
  private List<Integer> mReadFileList;
  private ReadType mReadType;
  private String mTfsAddress;

  public ReadThread(int id, List<Integer> readFileList, ReadType readType) {
    super(id);
    mThreadReport = new RWThreadReport();

    mContent = new byte[DEFALUT_READ_GRAIN];
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
