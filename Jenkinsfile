def dockerRepoName     = 'm3-integration'
def serviceName        = 'm3-integration'
def repoServiceName    = 'github.com'
def repoName           = "git@${repoServiceName}:legoly/${serviceName}.git"
def err                = null
def awsRegion          = 'us-east-1'
def awsClientId        = "682989504111"
def dockerRegistryName = "${awsClientId}.dkr.ecr.${awsRegion}.amazonaws.com"
def dockerRegistryUrl  = "https://${dockerRegistryName}"
def fileVersion       = "${serviceName}-build-name.txt"
def slackTeam         = 'teammagia'
currentBuild.result   = 'SUCCESS'

// To use slack notifications install Jenkins CI Slack App
// https://{{ SLACK_TEAM }}.slack.com/apps/A0F7VRFKN-jenkins-ci
def buildNotify(slackTeam, buildStatus, subject) {
    // build status of null means successful
    buildStatus =  buildStatus ?: 'SUCCESS'

    // Default values
    def colorCode = 'warning'
    def summary = "${subject} (<${env.BUILD_URL}|Open>)"

    // Override default values based on build status
    if (buildStatus == 'STARTED') {
      colorCode = 'warning'
    } else if (buildStatus == 'SUCCESS') {
      colorCode = 'good'
    } else {
      colorCode = 'danger'
    }

    // Send notifications
    slackSend color: colorCode,
              message: summary,
              teamDomain: "${slackTeam}",
              channel: '#jenkins',
              tokenCredentialId: 'slack-token'
}

node {
    properties([
           pipelineTriggers([pollSCM('H/2 * * * *')]),
           disableConcurrentBuilds()
    ])

    try {
        stage('Get Source') {
            // Do not clean workspace to prevent rebuilding python libs
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[$class: 'CleanCheckout'], [
                $class: 'SubmoduleOption',
                disableSubmodules: false,
                parentCredentials: true,
                recursiveSubmodules: true,
                reference: '',
                trackingSubmodules: false
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[credentialsId: 'mykola-github-key', url: "${repoName}"]]
            ])

            // Change build display name
            gitCommit = sh(returnStdout: true, script: 'git rev-parse HEAD').trim()
            shortCommit = gitCommit.take(7)

            currentBuild.displayName = "${BUILD_NUMBER}-${shortCommit}"

            gitCommitAuthor = sh(returnStdout: true, script: 'git show --format="%aN" ${gitCommit} | head -1').trim()

            buildNotify "${slackTeam}", 'STARTED', "${env.JOB_NAME} - ${currentBuild.displayName} Started by changes from ${gitCommitAuthor}"

            writeFile file: "${fileVersion}", text: "${currentBuild.displayName}"
       }


        stage('Build and Publish Docker Image') {
           docker.withRegistry("${dockerRegistryUrl}") {
                def image = docker.build("${dockerRepoName}:${currentBuild.displayName}")
                image.push()
                image.push 'latest'
            }
        }
    }
    catch (caughtError) {
        currentBuild.result = "FAILURE"
        throw caughtError
    }
    finally {
        buildNotify "${slackTeam}", "${currentBuild.result}", "${env.JOB_NAME} - ${currentBuild.displayName} ${currentBuild.result}"

        // Remove docker images from jenkins
        sh """
            [ -z "\$(docker images -q ${dockerRegistryName}/${dockerRepoName}:${currentBuild.displayName})" ] || docker rmi "${dockerRegistryName}/${serviceName}:${currentBuild.displayName}"
            [ -z "\$(docker images -q ${dockerRegistryName}/${dockerRepoName}:latest)" ] || docker rmi "${dockerRegistryName}/${serviceName}:latest"
            [ -z "\$(docker images -q ${dockerRepoName}:${currentBuild.displayName})" ] || docker rmi "${serviceName}:${currentBuild.displayName}"
        """

        archiveArtifacts artifacts: "${fileVersion}",
                         onlyIfSuccessful: true
    }
}

