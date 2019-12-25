#!/usr/bin/env bash
set -xe
env
source /root/.bash_profile
echo $PATH
Carbon_FOLDER="${PWD}"
git log -3

# get carbon branch from 'CID_REPO_BRANCH' or 'CID_TAG_INFO'
if  [ ! -n "${CID_REPO_BRANCH}" ] ;then
    CARBON_BRANCH=${CID_TAG_INFO}
else
    CARBON_BRANCH=${CID_REPO_BRANCH}
fi
echo "Carbon branch: ${CARBON_BRANCH}"

export COMPONENT_VERSION=${COMPONENT_VERSION:-1.6.1.0100}
export DP_VERSION=${DP_VERSION:-dplatform}
export HADOOP_VERSION=${HADOOP_VERSION:-2.7.2.0100-dplatform}
export SPARK_VERSION=${SPARK_VERSION:-2.3.2.0101-hw-2.0.0.dli-SNAPSHOT}
export BUILD_VERSION=${COMPONENT_VERSION}-${DP_VERSION}
export CI_LOCAL_REPOSITORY="carbon_local_repository"
#mvn -s /home/tool/apache-maven-3.3.3/conf/carbon_settings.xml clean install -U -Pbuild-with-format -Pspark-2.3 -Pmv -DskipTests -Dfindbugs.skip=true -Dcheckstyle.skip=true
#-Dspark.version=${SPARK_VERSION} -Dhadoop.version=${HADOOP_VERSION} -Dbuild.version=${BUILD_VERSION}

sed -i "s/<localRepository>.*/<localRepository>${CI_LOCAL_REPOSITORY}<\/localRepository>/" .build_config/carbon_settings.xml

echo "
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration>
          <forkCount>10</forkCount>
          <reuseForks>true</reuseForks>
        </configuration>
      </plugin>" > s.txt
keyWord="<pluginManagement>"
r1=`grep -n ${keyWord} pom.xml|awk -F: '{print $1}'|head -1`
((r2=r1+1))
sed -i "${r2} r s.txt" pom.xml
rm s.txt -rf

junitPluginType=$1
echo "Junit Plugin Type $junitPluginType"

if [[ ${junitPluginType} =~ "jacoco" ]]
then
cd ${Carbon_FOLDER}
mvn_param="-fae  -U -Pbuild-with-format -Pspark-2.3 -Pmv -Dfindbugs.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip=true -Dmaven.test.failure.ignore=true -Dspark.version=${SPARK_VERSION} -Dhadoop.version=${HADOOP_VERSION} -Dbuild.version=${BUILD_VERSION} -s ./.build_config/carbon_settings.xml"
mvn clean install $mvn_param
echo "Test finished"
mvn $mvn_param surefire-report:report-only -Daggregate=true
echo "Report Finished."

elif [[ ${junitPluginType} =~ "cobertura" ]]
then
echo -e "
	<plugin>
		<groupId>org.codehaus.mojo</groupId>
		<artifactId>cobertura-maven-plugin</artifactId>
		<version>2.7</version>
		<configuration>
			<formats>
				<format>html</format>
				<format>xml</format>
			</formats>
			<aggregate>true</aggregate>
		</configuration>
	</plugin>" > s.txt
keyWord="<artifactId>maven-checkstyle-plugin</artifactId>"
r1=`grep -n ${keyWord} pom.xml|awk -F: '{print $1}'|head -1`
((r2=r1-3))
sed -i "${r2} r s.txt" pom.xml
rm s.txt -rf

cd ${Carbon_FOLDER}
mvn_param="-fae -U -Pbuild-with-format -Pspark-2.3 -Pmv -Dfindbugs.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip=true -Dmaven.test.failure.ignore=true -Dspark.version=${SPARK_VERSION} -Dhadoop.version=${HADOOP_VERSION} -Dbuild.version=${BUILD_VERSION} -s ./.build_config/carbon_settings.xml"
mvn clean install $mvn_param cobertura:cobertura
echo "Test finished"
mvn $mvn_param surefire-report:report-only -Daggregate=true
echo "Report Finished."
else
    echo "error parameter!"
fi