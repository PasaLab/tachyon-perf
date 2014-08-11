Tachyon-Perf
============

A  general performance test framework for [Tachyon](http://tachyon-project.org/).The master branch is in version 0.1-SNAPSHOT.

##Getting Start
###Prerequisites
As this project is a test framework for Tachyon, you need to get the Tachyon installed first. If you are not clear how to setup Tachyon, please refer to the guideliens [here](http://tachyon-project.org/Running-Tachyon-on-a-Cluster.html). We support  tachyon-0.5.0 currently.

###Compile Tachyon-Perf
1. The default Tachyon version is set to 0.5.0. If you run a different version Tachyon, please copy the jar to the correct path. You can see 'lib/org/tachyonproject/tachyon/0.5.0/' as example.
2. The compiling command is 'mvn install', and you can specify the Tachyon version by '-Dtachyon.version=X.X.X' as a compile parameter.

###Run Tachyon-Perf Tests
1. Copy conf/tachyon-perf-env.sh.template to conf/tachyon-perf-env.sh and configure it.
2. Edit conf/slaves and distribute the tachyon-perf directory to all the same path on the slave nodes.
3. The running command is './bin/tachyon-perf <Read|Write> <READTYPE|WRITETYPE>'
 * The first parameter is Read or Write, means the read test or the write test.
 * The second parameter is the read type or the write type, which is defined in Tachyon. For example, CACHE(read type) or CACHE_THROUGH(write type).
4. The result report is generated at result/ as default.

###Configuration
Here lists the alternative configurations in conf/tachyon-perf-env.sh
| Property Name                        | Meaning                                                   |
| -------------------------------------|:---------------------------------------------------------:|
| tachyon.perf.tfs.address             | the Tachyon Master address, like tachyon://master:19998   |
| tachyon.perf.tfs.dir                 | the workspace dir in Tachyon File System                  |
| tachyon.perf.read.files.per.thread   | the number of files to read for each read thread          |
| tachyon.perf.read.identical          | if true, all the read threads will read the same files    |
| tachyon.perf.read.mode               | the read mode of read test, should be RANDOM or SEQUENCE  |
| tachyon.perf.read.threads.num        | the threads num of read test                              |
| tachyon.perf.write.file.length.bytes | the file size of write test, in bytes                     |
| tachyon.perf.write.files.per.thread  | the number of files to write for each write thread        |
| tachyon.perf.write.threads.num       | the threads num of write test                             |
| tachyon.perf.out.dir                 | the report output path                                    |
