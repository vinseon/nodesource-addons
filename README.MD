# ActiveEon ProActive Workflows and Scheduling 
## How to build
1) Import Gradle project

2) Build shadowJar with ./gradlew shadowJar -PexcludeRuntimeDependencies

## How to install
1) Copy .jar file from build/libs into ProACtive Workflows and Scheduling Addons folder
2) Add 'org.ow2.proactive.resourcemanager.nodesource.infrastructure.AWSEC2Infrastructure' to [ProActive_Home]config/rm/nodesource/infrastructures
