#!/bin/sh
# Local dev entrypoint for docker-compose: compiles the job from source and runs it once.
set -e

# feedback-viewer's classes jar isn't published anywhere, so build it from the mounted checkout,
# the same way CI does.
if [ ! -f /viewer/pom.xml ]; then
  echo "feedback-viewer not found. Check it out next to this repo or set VIEWER_DIR." >&2
  exit 1
fi
mvn -q -DskipTests -Dspotless.check.skip=true -f /viewer/pom.xml install

# airtable.java isn't on Maven Central; install it again only when lib/ has a newer copy.
jar=lib/airtable.java-0.2.0.jar
installed=/root/.m2/repository/com/sybit/airtable.java/0.2.0/airtable.java-0.2.0.pom
if [ ! -f "$installed" ] || [ "$jar" -nt "$installed" ]; then
  mvn -q install:install-file -Dfile="$jar" -DgroupId=com.sybit -DartifactId=airtable.java \
    -Dversion=0.2.0 -Dpackaging=jar -DgeneratePom=true
fi

exec mvn -q spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Xmx2g -agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND:-n},address=*:8000"
