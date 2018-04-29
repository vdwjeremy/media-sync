from repositories import Local as LocalRepo
#from repositories import db
#import os
import os.path


dir1 = os.path.abspath('folder1')
dir2 = os.path.abspath('folder2')
wd = os.path.abspath('workspace')
os.chdir(wd)

repos = [
    LocalRepo('folder1', dir1),
    LocalRepo('folder2', dir2)
]


# refresh repositories
for repo in repos:
    repo.refresh()


#db.close()
