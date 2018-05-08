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
        chunk = f.read(100*1024)
        while len(chunk) > 0:
            h.update(chunk)
            chunk = f.read(100 * 1024)
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
        global db
        print('adding %s' % ref)
        file_hash = hash_file(tmp_file_path)
        date_taken = find_date_taken(tmp_file_path)
        # Insert a row of data
        db.execute("INSERT INTO %s VALUES (?, ?, ?)" % self.name, (ref, file_hash, date_taken))
        db.commit()
        #shutil.move(tmp_file_path, file_hash) # in case we want to reuse it after
        os.remove(tmp_file_path)

    # returns true if the media identified as 'ref' is known in the repository
    def has(self, ref):
        global db
        c = db.cursor()
        c.execute('SELECT ref FROM %s WHERE ref=?' % self.name, (ref,))
        return c.fetchone() is not None

    # go through all hashes and eventually reorganize/complete files (depending on the implementation)
    def standardize(self):
        global db
        c = db.cursor()
        c.execute('SELECT DISTINCT hash FROM %s' % self.name)
        with open('%s.hash.list' % self.name, 'w') as f:
            row = c.fetchone()
            while row is not None:
                f.write(row[0] + '\n')
                row = c.fetchone()
        with open('%s.hash.list' % self.name, 'r') as f:
            for line in f:
                h = line.strip()
                c.execute('SELECT ref, date_taken FROM %s WHERE hash=?' % self.name, (h,))
                self.standardize_single_hash(h, c.fetchall())

    def delete(self, ref):
        print('deleting %s' % ref)
        global db
        db.execute('DELETE FROM %s WHERE ref=?' % self.name, (ref,))
        db.commit()

    def rename(self, ref, new_ref):
        print('renaming %s -> %s' % (ref, new_ref))
        global db
        db.execute('UPDATE %s SET ref=? WHERE ref=?' % self.name, (new_ref, ref))
        db.commit()

    def sync_from(self, other):
        print('cross upload %s -> %s' % (other.name, self.name))
        global db
        c = db.cursor()
        c.execute('SELECT DISTINCT hash FROM %s EXCEPT SELECT DISTINCT hash FROM %s' % (other.name, self.name))
        with open('%s_to_%s.hash.list' % (other.name, self.name), 'w') as f:
            row = c.fetchone()
            while row is not None:
                f.write(row[0] + '\n')
                row = c.fetchone()
        c.close()
        with open('%s_to_%s.hash.list' % (other.name, self.name), 'r') as f:
            for line in f:
                h = line.strip()
                c = db.cursor()
                c.execute('SELECT ref FROM %s WHERE hash=?' % other.name, (h,))
                ref = str(c.fetchone()[0])
                c.close()
                tmp_file = other.download(ref)
                self.upload(tmp_file)


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

    def standardize_single_hash(self, file_hash,  medias):
        main_file = None
        dupes = []
        # check existence
        for (ref, date_taken) in medias:
            if not os.path.isfile(os.path.join(self.path, ref)):
                self.delete(ref)
            elif main_file is None and not str(ref).startswith('dupe'):
                main_file = (ref, date_taken)
            else:
                dupes.append((ref, date_taken))
        # elect a master
        if main_file is None:
            if len(dupes) == 0:
                print('no valid file found for %s' % file_hash)
                return
            else:
                main_file = dupes.pop()
        # standardize master
        path = str(main_file[0])
        date = datetime.fromtimestamp(int(main_file[1]))
        std_path = os.path.join(date.strftime('%Y'),
                                date.strftime('%Y-%m-%d %H-%M-%S') + os.path.splitext(path)[1])
        self.move(path, std_path)
        # standardize dupes
        for dupe in dupes:
            path = str(dupe[0])
            date = datetime.fromtimestamp(int(dupe[1]))
            std_path = os.path.join('dupes',
                                    date.strftime('%Y'),
                                    date.strftime('%Y-%m-%d %H-%M-%S') + os.path.splitext(path)[1])
            self.move(path, std_path)

    def move(self, from_path, to_path):
        if from_path == to_path:
            return
        new_path = to_path
        num = 1
        while os.path.isfile(os.path.join(self.path, new_path)):
            new_path = (' %i' % num).join(os.path.splitext(to_path))
            num += 1
            if from_path == new_path:
                return
        directory = os.path.dirname(os.path.join(self.path, new_path))
        if not os.path.exists(directory):
            os.makedirs(directory)
        shutil.move(os.path.join(self.path, from_path), os.path.join(self.path, new_path))
        self.rename(from_path, new_path)

    def check_access(self):
        # TODO
        return True

    # 'ref' is the id as defined in the database
    # return the path to the local file
    def download(self, ref):
        dst = os.path.basename(ref)
        shutil.copy2(os.path.join(self.path, ref), dst)
        return dst

    # 'path' is the path to the local file to upload
    def upload(self, local_file_path):
        date = datetime.fromtimestamp(find_date_taken(local_file_path))
        std_path = os.path.join(date.strftime('%Y'),
                                date.strftime('%Y-%m-%d %H-%M-%S') + os.path.splitext(local_file_path)[1])
        new_path = std_path
        num = 1
        while os.path.isfile(os.path.join(self.path, new_path)):
            new_path = (' %i' % num).join(os.path.splitext(std_path))
            num += 1

        directory = os.path.dirname(os.path.join(self.path, new_path))
        if not os.path.exists(directory):
            os.makedirs(directory)
        shutil.copy2(local_file_path, os.path.join(self.path, new_path))
        self.add_file(new_path, local_file_path)


