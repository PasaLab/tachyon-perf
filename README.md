Tachyon-Perf
============

A  general performance test framework for [Tachyon](http://tachyon-project.org/).The master branch is in version 0.1-SNAPSHOT.

##Prerequisites
As this project is a test framework for Tachyon, you need to get the Tachyon installed first. If you are not clear how to setup Tachyon, please refer to the guidelines [here](http://tachyon-project.org/Running-Tachyon-on-a-Cluster.html). We support  tachyon-0.5.0 currently.

##Compile Tachyon-Perf
1. The default Tachyon version is set to 0.5.0. If you run a different version Tachyon, please copy the jar to the correct path. You can see `lib/org/tachyonproject/tachyon/0.5.0/` as example.
2. The compiling command is `mvn install`, and you can specify the Tachyon version by `-Dtachyon.version=X.X.X` as a compile parameter.

##Run Tachyon-Perf Tests
1. Copy `conf/tachyon-perf-env.sh.template` to `conf/tachyon-perf-env.sh` and configure it.
2. Edit `conf/slaves` and distribute the tachyon-perf directory to all the same path on the slave nodes.
3. The running command is `./bin/tachyon-perf <TaskType> [args...]`
 * The first parameter is the type of test task, and now it should be `Read` or `Write`, means the read test or the write test.
 * The following parameters are the args for the test task, and now it should be the read type for Read task or the write type for Write task. The read type and write type are defined in Tachyon, for example, `CACHE(read type)` or `CACHE_THROUGH(write type)`.
4. When TachyonPerf is running, the status of the test job will be printed on the console. For some reasons, if you want to abort the tests, you can just press `Ctrl + C` to terminate current thread and then type the command `./bin/tachyon-perf-abort` at the master node to abort test processes on each slave node.
5. After all the tests finished successfully, each node will generate a result report, locates at `result/` by default. You can also generate a graphical report by following commands in the section "Generating Test Reports".

##Configuration
Here, we list the alternative configurations in `conf/tachyon-perf-env.sh`
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
        <td>tachyon.perf.read.files.per.thread</td>
        <td>the number of files to read for each read thread</td>
        <td>10</td>
    </tr>
    <tr>
        <td>tachyon.perf.read.identical</td>
        <td>if true, all the read threads will read the same files</td>
        <td>false</td>
    </tr>
    <tr>
        <td>tachyon.perf.read.mode</td>
        <td>the read mode of read test, should be RANDOM or SEQUENCE</td>
        <td>RANDOM</td>
    </tr>
    <tr>
        <td>tachyon.perf.read.threads.num</td>
        <td>the threads num of read test</td>
        <td>4</td>
    </tr>
    <tr>
        <td>tachyon.perf.write.file.length.bytes</td>
        <td>the file size of write test, in bytes</td>
        <td>128 * tachyon.Constants.MB</td>
    </tr>
    <tr>
        <td>tachyon.perf.write.files.per.thread</td>
        <td>the number of files to write for each write thread</td>
        <td>10</td>
    </tr>
    <tr>
        <td>tachyon.perf.write.threads.num</td>
        <td>the threads num of write test</td>
        <td>4</td>
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


