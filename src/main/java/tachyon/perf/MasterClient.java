package tachyon.perf;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.thrift.MasterService;
import tachyon.perf.thrift.SlaveAlreadyRegisterException;
import tachyon.perf.thrift.SlaveNotRegisterException;

public class MasterClient implements Closeable {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);
  private static final int MAX_CONNECT_TRY = 5;

  private final String mMasterHostname;
  private final int mMasterPort;

  private MasterService.Client mClient = null;
  private TProtocol mProtocol = null;
  private volatile boolean mConnected;

  public MasterClient() {
    mMasterHostname = PerfConf.get().TACHYON_PERF_MASTER_HOSTNAME;
    mMasterPort = PerfConf.get().TACHYON_PERF_MASTER_PORT;
    mConnected = false;

    mProtocol =
        new TBinaryProtocol(new TFramedTransport(new TSocket(mMasterHostname, mMasterPort)));
    mClient = new MasterService.Client(mProtocol);
  }

  @Override
  public synchronized void close() throws IOException {
    if (mConnected) {
      LOG.info("Disconnecting from Tachyon-Perf Master " + mMasterHostname + ":" + mMasterPort);
      mConnected = false;
    }
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
  }

  private synchronized boolean connect() {
    if (!mConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        return false;
      }
      mConnected = true;
      LOG.info("Connect to Tachyon-Perf Master " + mMasterHostname + ":" + mMasterPort);
    }
    return mConnected;
  }

  public synchronized void mustConnect() throws IOException {
    int tries = 0;
    while (tries ++ < MAX_CONNECT_TRY) {
      if (connect()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the Tachyon-Perf Master");
  }

  public synchronized boolean slave_canRun(int taskId, String nodeName) throws IOException {
    mustConnect();
    try {
      return mClient.slave_canRun(taskId, nodeName);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized void slave_finish(int taskId, String nodeName, boolean successFinish)
      throws IOException {
    mustConnect();
    try {
      mClient.slave_finish(taskId, nodeName, successFinish);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public void slave_ready(int taskId, String nodeName, boolean successSetup) throws IOException {
    mustConnect();
    try {
      mClient.slave_ready(taskId, nodeName, successSetup);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public boolean slave_register(int taskId, String nodeName, String cleanupDir) throws IOException {
    if (!connect()) {
      return false;
    }
    try {
      return mClient.slave_register(taskId, nodeName, cleanupDir);
    } catch (SlaveAlreadyRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
