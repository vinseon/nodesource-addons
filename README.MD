# ActiveEon ProActive Workflows and Scheduling 
[![Build Status](http://jenkins.activeeon.com/buildStatus/icon?job=nodesource-addons)](http://jenkins.activeeon.com/job/nodesource-addons/)

## How to build
1) Import Gradle project

2) Build shadowJar with ./gradlew shadowJar

## How to install
For example for the AWS EC2 infrastructure:
1) Copy .jar file from infrastructures/infrastructure-aws-ec2/build/libs into ProACtive Workflows and Scheduling Addons folder
2) Add 'org.ow2.proactive.resourcemanager.nodesource.infrastructure.AWSEC2Infrastructure' to [ProActive_Home]config/rm/nodesource/infrastructures
