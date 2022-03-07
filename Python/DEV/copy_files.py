import os
import shutil

src = "/Users/benjamin/Documents/Projets/OLD/Sorare-data/output/python/json"
dest = "/Users/benjamin/Documents/Projets/project-sorare-data/output/python/json"

src_files = os.listdir(src)

for file_name in src_files:
    full_file_name = os.path.join(src, file_name)
    if os.path.isfile(full_file_name):
        shutil.copy(full_file_name, dest)