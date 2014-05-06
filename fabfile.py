from fabric.api import local, run, cd, put, env, path
from fabric.contrib.project import rsync_project

env.use_ssh_config = True

def sync_source():
    rsync_project("/opt/gush/gush-sbt", "./", delete=True, exclude=[".git", "gush-intro", "logs", "target", "project", "mysql-bin.*", "gush.config.yml"], extra_opts="--exclude-from .gitignore")

def compile_project():
    with cd("/opt/gush/gush-sbt"), path("/opt/sbt/bin"):
        run("sbt compile")

def restart():
    run('/etc/init.d/gush stop')
    run('/etc/init.d/gush start')
    
def deploy():
    target_dir = '/opt/gush/gush-sbt'

    with cd(target_dir):
        sync_source()
        compile_project()

    restart()
