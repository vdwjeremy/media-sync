import sqlite3
import os
import os.path
import shutil
import hashlib
import exifread
from datetime import datetime


db = None


def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        chunck = f.read(100*1024)
        while len(chunck) > 0:
            h.update(chunck)
            chunck = f.read(100 * 1024)
    return h.hexdigest()


# find media date
# return as posix timestamp (seconds since 1970)
def find_date_taken(path):
    with open(path, 'rb') as f:
        tags = exifread.process_file(f, details=False)
        for tag in ['EXIF DateTimeDigitized', 'EXIF DateTimeOriginal', 'Image DateTime']:
            if tag in tags:
                try:
                    return int(datetime.strptime(str(tags[tag]), '%Y:%m:%d %H:%M:%S').timestamp())
                except ValueError:
                    pass  # invalid date, try next one
    return int(os.path.getmtime(path))


class Repository:
    def __init__(self, name):
        self.name = name
        global db
        if db is None:
            db = sqlite3.connect('media.db')

        db.execute('''CREATE TABLE IF NOT EXISTS %s
                     (ref TEXT PRIMARY KEY, hash TEXT, date_taken INTEGER)''' % name)
        db.execute('''CREATE INDEX IF NOT EXISTS %s_hash_idx
                     ON %s(hash)''' % (name, name))
        db.commit()

    # add a file to the repository
    # 'ref' is its unique identifier
    # 'tmp_file_path' is the content of the file, it must must a copy as it will be moved/deleted by 'Repository'
    def add_file(self, ref, tmp_file_path):
        print('adding %s of size %ik' % (ref, os.path.getsize(tmp_file_path)/1024))
        file_hash = hash_file(tmp_file_path)
        date_taken = find_date_taken(tmp_file_path)
        # Insert a row of data
        db.execute("INSERT INTO %s VALUES (?, ?, ?)" % self.name, (ref, file_hash, date_taken))
        db.commit()
        shutil.move(tmp_file_path, file_hash)

    # returns true if the media identified as 'ref' is known in the repository
    def has(self, ref):
        global db
        c = db.cursor()
        c.execute('SELECT ref FROM %s WHERE ref=?' % self.name, (ref,))
        return c.fetchone() is not None


class Local(Repository):
    def __init__(self, name, path):
        Repository.__init__(self, name)
        self.path = path

    def refresh(self):
        for dirName, subdirList, fileList in os.walk(self.path):
            for fname in fileList:
                fpath = os.path.join(dirName, fname)
                ref = os.path.relpath(fpath, self.path)
                if not self.has(ref):
                    shutil.copy2(fpath, 'tmp')
                    self.add_file(ref, 'tmp')

