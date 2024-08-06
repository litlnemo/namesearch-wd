import os
import sys
import glob
import shutil
import subprocess as sb
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

usagemsg = f"Usage: {sys.argv[0]} <location of zipped names>"

if len(sys.argv) == 2:
    if not os.path.exists(sys.argv[1]):
        print(usagemsg)
        sys.exit(1)
    
    print(f"Unzipping file: {sys.argv[1]}")
    
    try:
        result = sb.check_call(['unzip', '-u', sys.argv[1], '-d', 'rawdata'])
        assert result == 0, "Had problem unpacking data"
        print("Unzipping successful.")
    except Exception as e:
        print(f"Error during unzipping: {e}")
        sys.exit(1)

    files = glob.glob('rawdata/*.txt')
    print(f"Found {len(files)} files to process.")

    data_frames = []

    for fname in files:
        print(f"Reading file: {fname}")
        try:
            x = pd.read_csv(fname, header=None, names=['name', 'gender', 'count'])
            x['year'] = int(fname[11:-4])  # Adjust this slicing if filename patterns change
            data_frames.append(x)
        except Exception as e:
            print(f"Error reading {fname}: {e}")
            continue

    try:
        print("Concatenating data frames...")
        data = pd.concat(data_frames, ignore_index=True)
        print(f"Data concatenation complete. Total records: {len(data)}")
    except Exception as e:
        print(f"Error during concatenation: {e}")
        sys.exit(1)

# Handle potential file IO errors
    chunk_size = 50000  # Adjust based on system capacity
    try:
        print("Saving to HDF5 format in chunks...")
        with pd.HDFStore('names.h5', complib='blosc', complevel=9) as fid:
            print("HDF5 store opened.")
            for start in range(0, len(data), chunk_size):
                end = min(start + chunk_size, len(data))
                fid.append('names', data.iloc[start:end])
                print(f"Saved chunk from {start} to {end}")
            print("Data saved to HDF5 successfully.")
        shutil.rmtree('rawdata')
        print("Raw data folder removed.")
    except Exception as e:
        print(f"Error saving to HDF5: {e}")
        sys.exit(1)


    
    """
    try:
        print("Saving to HDF5 format...")
        with pd.HDFStore('names.h5', complib='blosc', complevel=9) as fid:
            print("HDF5 store opened.")
            fid['names'] = data
            print("Data saved to HDF5 successfully.")
        shutil.rmtree('rawdata')
        print("Raw data folder removed.")
    except Exception as e:
        print(f"Error saving to HDF5: {e}")
        sys.exit(1)
        """

elif not os.path.exists('names.h5'):
    print(usagemsg)
    sys.exit(-1)

# Further processing of the data
fid = pd.HDFStore('names.h5')
data = fid['names']
fid.close()

data = data[data['gender'] == 'F']
gb = data.groupby('name')

# Using sort_values instead of sort
agged = gb.agg({'count': np.sum, 'year': np.mean}).sort_values('count', ascending=False)

print("10 most popular names over the last 132 years:")
print(agged.iloc[:10])

print('')

print("10 least popular names over the last 132 years:")
print(agged.iloc[-10:])

plt.semilogy(agged['count'])
plt.grid(True)
plt.title('Sorted name histogram over the last 132 years')
plt.show()

print("Proportion of female babies given any of the 50 most popular names:")
print(np.sum(agged['count'][:50]) / np.sum(agged['count']))

auri_count = agged.loc['Auri', 'count'] if 'Auri' in agged.index else None
auri_index = agged.index.get_loc('Auri') if 'Auri' in agged.index else None

def get(num=50, at=17000):
    return agged.index[at:at+num]

print(' '.join(agged[agged.index.map(len) == 4].index[200:300]))
