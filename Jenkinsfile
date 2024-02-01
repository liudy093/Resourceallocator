pipeline {
  environment {
        registry = "https://harbor.cloudcontrolsystems.cn/workflow"
  }
  agent any
  stages {
        stage('Cloning Git') {
            steps {
                git branch: 'master', url: 'gitea@git.cloudcontrolsystems.cn:CloudTeam/ResourceAllocator.git'
            }
        }
        stage('Building image') {
            steps{
                    script {
                        customImage = docker.build("harbor.cloudcontrolsystems.cn/workflow/resource-allocator:latest")
                    }
            }
        }
        stage('Deploy') {
            steps{
                    script {
                        docker.withRegistry(registry, '516bc90a-d2eb-4464-b90f-60121f7df34c') {
                            customImage.push()
                        }
                    }
            }
        }
    }
}