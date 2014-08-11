#!/usr/bin/env bash

# The following gives an example:

if [[ `uname -a` == Darwin* ]]; then
  # Assuming Mac OS X
  export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home)}
  export TACHYON_PERF_JAVA_OPTS="-Djava.security.krb5.realm= -Djava.security.krb5.kdc="
else
  # Assuming Linux
  if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-7-oracle
  fi
fi

export JAVA="$JAVA_HOME/bin/java"

export TACHYON_CONF_DIR_SH=/home/hadoop/tachyon-install/tachyon-0.5.0/conf/tachyon-env.sh

CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export TACHYON_PERF_JAVA_OPTS+="
  -Dlog4j.configuration=file:$CONF_DIR/log4j.properties
  -Dtachyon.perf.tfs.address=tachyon://slave021:19998
  -Dtachyon.perf.tfs.dir=/tachyon-perf
  -Dtachyon.perf.read.files.per.thread=4
  -Dtachyon.perf.read.identical=true
  -Dtachyon.perf.read.mode=RANDOM
  -Dtachyon.perf.read.threads.num=3
  -Dtachyon.perf.write.file.length.bytes=134217728
  -Dtachyon.perf.write.files.per.thread=5
  -Dtachyon.perf.write.threads.num=2
  -Dtachyon.perf.out.dir=$TACHYON_PERF_HOME/results
"
