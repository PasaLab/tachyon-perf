Tachyon-Perf
============

A  general performance test framework for [Tachyon](http://tachyon-project.org/).The master branch is in version 0.1-SNAPSHOT.

##Prerequisites
As this project is a test framework for Tachyon, you need to get the Tachyon installed first. If you are not clear how to setup Tachyon, please refer to the guidelines [here](http://tachyon-project.org/Running-Tachyon-on-a-Cluster.html). We support  tachyon-0.5.0 currently.

##Compile Tachyon-Perf
1. The compiling command is `mvn install`, and you can specify the Tachyon version or Hadoop version by `-Dtachyon.version=X.X.X` or `-Dhadoop.version=X.X.X` as a compile parameter.
2. The default Tachyon version is set to 0.5.0, which is now available from MVNRepository. The default Hadoop version is set to 1.0.4, which is also default in Tachyon-0.5.0.

##Run Tachyon-Perf Tests
1. Copy `conf/tachyon-perf-env.sh.template` to `conf/tachyon-perf-env.sh` and configure it.
2. Edit `conf/slaves` and distribute the tachyon-perf directory to all the same path on the slave nodes.
3. The running command is `./bin/tachyon-perf <TaskType>`
 * The parameter is the type of test task, and now it should be `Read` or `Write`, means the read test or the write test.
 * The task's configurations are in `conf/<TaskType>.xml`, and you can modify it as your wish. Now there has `conf/Read.xml` and `conf/Write.xml`.
4. When TachyonPerf is running, the status of the test job will be printed on the console. For some reasons, if you want to abort the tests, you can just press `Ctrl + C` to terminate current thread and then type the command `./bin/tachyon-perf-abort` at the master node to abort test processes on each slave node.
5. After all the tests finished successfully, each node will generate a result report, locates at `result/` by default. You can also generate a graphical report by following commands in the section "Generating Test Reports".

##Configuration
Here, we list the alternative configurations in `conf/tachyon-perf-env.sh`. The detailed description of task configurations are in their xml files.
<table>
    <tr>
        <td><b>Property Name</b></td>
        <td><b>Meaning</b></td>
        <td><b>Default Value</b></td>
    </tr>
    <tr>
        <td>tachyon.perf.status.debug</td>
        <td>if true, the node names of the running status will be printed</td>
        <td>false</td>
    </tr>
    <tr>
        <td>tachyon.perf.failed.abort</td>
        <td>if true, the test will abort when the number of failed nodes is more than a threshold</td>
        <td>true</td>
    </tr>
    <tr>
        <td>tachyon.perf.failed.percentage</td>
        <td>the percentage to determine the failed nodes threshold</td>
        <td>1</td>
    </tr>
    <tr>
        <td>tachyon.perf.tfs.address</td>
        <td>the Tachyon Master address</td>
        <td>tachyon://localhost:19998</td>
    </tr>
    <tr>
        <td>tachyon.perf.tfs.dir</td>
        <td>the workspace dir in Tachyon File System</td>
        <td>/tachyon-perf-workspace</td>
    </tr>
    <tr>
        <td>tachyon.perf.out.dir</td>
        <td>the report output path</td>
        <td>TACHYON_PERF_HOME + "/result"</td>
    </tr>
</table>

##Generating Test Reports
There are two tools used to generate a report. One is `TachyonPerfCollector`, which will generate a text report. And another is `TachyonPerfHtmlReport`, which will generate a visible html report.
###TachyonPerfCollector
After a test task is finish, you can use generate a text report at `$TACHYON_PERF_OUT_DIR/` with command `./bin/tachyon-perf-collect <TaskType>`.
###TachyonPerfHtmlReport
To make the test report easy and friendly to read, we demostrate the test results with charts and diagrams by adopting D3 javascript. After running both write and read tests(all tests are  finished), you can generate an HTML report with command `./bin/tachyon-perf-html-report`. Then the report will be generated at `$TACHYON_PERF_OUT_DIR/htmlReport/report.html`. It's in the HTML format so you can open it with your web browser.


