#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

#JAVA_OPTS="-Djava.security.debug=access -Djava.security.manager -Djava.security.policy=/$PATH_TO_CODE_BASE/java.policy -Djava.rmi.server.useCodebaseOnly=false"	
JAVA_OPTS="--add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

MAIN_CLASS="ar.edu.itba.pod.server.Server"


java  $JAVA_OPTS -cp 'lib/jars/*' $MAIN_CLASS $*