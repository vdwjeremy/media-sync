from repositories import Local as LocalRepo
import os.path

script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(script_dir, 'workspace'))

# each cluster is isolated
# each cluster as a set of repositories which share the same files
clusters = [
    # test
    #[
    #    LocalRepo('folder1', os.path.join(script_dir, 'folder1'), 'wGn*$g63ZXs^peTJGPw&yV7TS#'),
    #    LocalRepo('folder2', os.path.join(script_dir, 'folder2'), 'BGr4B5SZjr*HsdRztJ2qCbyJ$9'),
    #    LocalRepo('nas_test', 'X:\\Photos\\test', 'wGn*$g63ZXs^peTJGPwcyV7TS#')
    #],
    #[
    #    LocalRepo('local_2015_2020', 'C:\\Users\\jvandewoestyne\\Pictures\\Photos 2015-2020', 'Bq5vHk3aqks5Y$BKX6DQsQBxTg'),
    #    LocalRepo('backup_2015_2020', 'W:\\Photos\\2015-2020', 'Wa26AEk!nENxb5EZ3Cmv*j9&&Q'),
    #    LocalRepo('nas_2015_2020', 'Y:\\Photos\\2015-2020', 't*gq5V6QC*uayVp^2zwz#J!PJw')
    #]
    [
        LocalRepo('dupe1', '/media/data/nextcloud/Photos.dupes', None),
        LocalRepo('dupe2', '/media/data/nextcloud/Photos.dupes2', None)
    ]
]

for repos_orig in clusters:
    # check access
    repos = []
    for repo in repos_orig:
        if repo.check_access():
            repos.append(repo)
        else:
            print('no access to %s' % repo.name)
    # refresh repositories
    for repo in repos:
        repo.refresh()
    # standardize paths / dedupe files
    for repo in repos:
        repo.standardize()
    # cross upload
    for repo_src in repos:
        for repo_dst in repos:
            if repo_src != repo_dst:
                repo_dst.sync_from(repo_src)

#input("Press Enter to continue...")
