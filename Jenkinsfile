// parameters
// ----------
// gitTag

def replace = { File source, String toSearch, String replacement ->
  source.write(source.text.replaceAll(toSearch, replacement))
}
    
def checkoutTag(folderName, url, tagName) {
  dir(folderName) {
    deleteDir()
    checkout([
       $class: 'GitSCM', 
        branches: [[name: "refs/tags/${tagName}"]],
        doGenerateSubmoduleConfigurations: false,
        extensions: [],
        gitTool: 'Default',
        submoduleCfg: [],
        userRemoteConfigs: [[url: url]]
    ])
  }
}

def build(folderName, mavenRuntime, buildInfo) {
  dir (path: folderName) {
    mavenRuntime.run pom: 'pom.xml', goals: 'install', buildInfo: buildInfo
  }
}

def artifactoryDeploy(folderName, artifactoryServer, mavenRuntime, buildInfo) {
  dir (path: folderName) {
    mavenRuntime.deployer.deployArtifacts buildInfo
    artifactoryServer.publishBuildInfo buildInfo
  }
}

node ('master') {
  def artifactoryServer = Artifactory.server 'prod'
  def mavenRuntime = Artifactory.newMavenBuild()
  def buildInfo

  stage ('Artifactory configuration') {
    mavenRuntime.tool = 'm3' 
    mavenRuntime.deployer releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local', server: artifactoryServer
    mavenRuntime.resolver releaseRepo: 'repo', snapshotRepo: 'repo', server: artifactoryServer
    mavenRuntime.deployer.deployArtifacts = false
    buildInfo = Artifactory.newBuildInfo()
  }

  stage ('Checkout') {
    checkoutTag('jeometry', 'ssh://git@github.com/jeometry-org/jeometry.git', '0-GBASITES-${gitTag}');
    checkoutTag('revolsys', 'ssh://git@github.com/revolsys/com.revolsys.open.git', '0-GBASITES-${gitTag}');
    checkoutTag('gba', 'ssh://git@github.com/pauldaustin/ca.bc.gov.gba.git', '0-GBASITES-${gitTag}');
    checkoutTag('gba-sites', 'ssh://git@github.com/pauldaustin/gba-sites.git', '${gitTag}');
  }

  stage ('Maven Install') {
    build('jeometry', mavenRuntime, buildInfo);
    build('revolsys', mavenRuntime, buildInfo);
    build('gba', mavenRuntime, buildInfo);
    build('gba-sites', mavenRuntime, buildInfo);
  }

  stage ('Artifactory Deploy') {
    artifactoryDeploy('jeometry', artifactoryServer, mavenRuntime, buildInfo);
    artifactoryDeploy('revolsys', artifactoryServer, mavenRuntime, buildInfo);
    artifactoryDeploy('gba', artifactoryServer, mavenRuntime, buildInfo);
    artifactoryDeploy('gba-sites', artifactoryServer, mavenRuntime, buildInfo);
  }
}
