from repositories import Local as LocalRepo
#from repositories import db
#import os
import os.path


dir1 = os.path.abspath('folder1')
dir2 = os.path.abspath('folder2')
wd = os.path.abspath('workspace')
os.chdir(wd)

# test
repos = [
    LocalRepo('folder1', dir1),
    LocalRepo('folder2', dir2)
]

# prod
repos = [
    LocalRepo('atrier', 'C:\\Users\\jvandewoestyne\\Pictures\\photos a trier')
]

# refresh repositories
for repo in repos:
    repo.refresh()
# standardize paths / dedupe files
for repo in repos:
    repo.standardize()


#db.close()
