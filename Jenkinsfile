// parameters
// ----------
// rsBranch
// gbaBranch
// gbaSitesBranch
// gitTag

def replace = { File source, String toSearch, String replacement ->
  source.write(source.text.replaceAll(toSearch, replacement))
}
    
def checkoutBranch(folderName, url, branchName) {
  dir(folderName) {
    deleteDir()
    checkout([
      $class: 'GitSCM',
      branches: [[name: branchName]],
      doGenerateSubmoduleConfigurations: false,
      extensions: [],
      gitTool: 'Default',
      submoduleCfg: [],
      userRemoteConfigs: [[url: url]]
    ])
  }
}

def setVersion(folderName, prefix) {
  def tagName = "${gitTag}";
  if (prefix != null) {
    tagName = "${prefix}-${gitTag}";
  }
  dir(folderName) {
    sh "git checkout -B version-${tagName}"
    withMaven(jdk: 'openjdk-11', maven: 'm3') {
      sh "mvn versions:set -DnewVersion='${tagName}' -DgenerateBackupPoms=false"
    }
    sh "sed -i 's/<org.jeometry.version>.*<\\/org.jeometry.version>/<org.jeometry.version>0-GBASITES-${gitTag}<\\/org.jeometry.version>/g' pom.xml"
    sh "sed -i 's/<com.revolsys.open.version>.*<\\/com.revolsys.open.version>/<com.revolsys.open.version>0-GBASITES-${gitTag}<\\/com.revolsys.open.version>/g' pom.xml"
    sh "sed -i 's/<ca.bc.gov.gba.version>.*<\\/ca.bc.gov.gba.version>/<ca.bc.gov.gba.version>0-GBASITES-${gitTag}<\\/ca.bc.gov.gba.version>/g' pom.xml"
  }
}

def tagVersion(folderName, prefix) {
  def tagName = "${gitTag}";
  if (prefix != null) {
    tagName = "${prefix}-${gitTag}";
  }
  dir(folderName) {
    sh """
git commit -a -m "Version ${tagName}"
git tag -f -a ${tagName} -m "Version ${tagName}"
git push -f origin ${tagName}
    """
  }
}

node ('master') {
  def rtMaven = Artifactory.newMavenBuild()
  def buildInfo

  stage ('SCM globals') {
     sh '''
git config --global user.email "paul.austin@revolsys.com"
git config --global user.name "Paul Austin"
     '''
  }

  stage ('Checkout') {
    checkoutBranch('jeometry', 'ssh://git@github.com/jeometry-org/jeometry.git', '${rsBranch}');
    checkoutBranch('revolsys', 'ssh://git@github.com/revolsys/com.revolsys.open.git', '${rsBranch}');
    checkoutBranch('gba', 'ssh://git@github.com/pauldaustin/ca.bc.gov.gba.git', '${gbaBranch}');
    checkoutBranch('gba-sites', 'ssh://git@github.com/pauldaustin/gba-sites.git', '${gbaSitesBranch}');
  }

  stage ('Set Project Versions') {
    setVersion('jeometry', '0-GBASITES');
    setVersion('revolsys', '0-GBASITES');
    setVersion('gba', '0-GBASITES');
    setVersion('gba-sites', null);
  }

  stage ('Tag') {
    tagVersion('jeometry', '0-GBASITES');
    tagVersion('revolsys', '0-GBASITES');
    tagVersion('gba', '0-GBASITES');
    tagVersion('gba-sites', null);
  }
}
