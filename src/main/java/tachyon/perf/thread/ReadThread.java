package tachyon.perf.thread;

import java.io.IOException;
import java.util.List;

import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;

public class ReadThread extends PerfThread {
  private static final int DEFALUT_READ_GRAIN = 4194304;// 4MB

  private List<Integer> mReadFileList;
  private ReadType mReadType;
  private byte[] mContent;

  public ReadThread(int id, String tfsAddress, List<Integer> readFileList, ReadType readType) {
    super(id, tfsAddress);
    mReadFileList = readFileList;
    mReadType = readType;
    mContent = new byte[DEFALUT_READ_GRAIN];
  }

  @Override
  public void run() {
    mThreadReport.start();
    TachyonFS tachyonClient;
    try {
      tachyonClient = TachyonFS.get(mTfsAddress);
    } catch (IOException e) {
      LOG.error("Read Thread " + ID + " falied to connect Tachyon");
      throw new RuntimeException(e);
    }

    for (int fileId : mReadFileList) {
      try {
        TachyonFile file = tachyonClient.getFile(fileId);
        InStream is = file.getInStream(mReadType);
        int readLen;
        while ((readLen = is.read(mContent)) > 0) {
          mThreadReport.successContent(readLen);
        }
        is.close();
      } catch (IOException e) {
        LOG.error("Read thread " + ID + "failed to read file, FileId: " + fileId, e);
        continue;
      }
      mThreadReport.successFiles(1);
    }
    mThreadReport.end();
  }
}
