import os

# TODO: hardcoded for now
ROOT_PATH = [
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop/",
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop1/",
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop2/",
]

def abs_path(root):
    """
    input: root: absolute path. Need to be the same as the hdfs-site.xml
    output: [ {name: filename, size: None}, ...]
    """
    r = []
    for dirpath, dirnames, filenames in os.walk(root):
        if len(filenames) > 0:
            r += [dict([("path", "%s/%s" % (dirpath, f)), ("size", None)])  for f in filenames]
    return r


def size_info(file_list):
    for entry in file_list:
        entry['size'] = os.path.getsize(entry['path'])
    return sorted(file_list, key=lambda entry: entry['size'], reverse=True) #sort by size


def calc_total_size(file_list):
    return sum(entry['size'] for entry in file_list)


def get_takers(partition_dict, balanced_threshold):
    """
    return a list of partitions that are below the balanced threshold.

    """
    return [k for k,v in partition_dict.items() if v['partition_size'] > balanced_threshold]


def get_givers(partition_dict, balanced_threshold):
    """
    Return a list of partitions that are above the balanced threshold.
    """
    return [k for k,v in partition_dict.items() if v['partition_size'] < balanced_threshold]


if __name__=="__main__":
    partition_dict = {}

    for path in ROOT_PATH:
        file_lst = abs_path(path)
        file_lst = size_info(file_lst)
        partition_dict[path] = {'files': file_lst, 'partition_size': calc_total_size(file_lst)}


    balanced_threshold = sum(v['partition_size'] for k,v in partition_dict.items()) / float(len(ROOT_PATH))
    print balanced_threshold
    print get_takers(partition_dict, balanced_threshold)
    print get_givers(partition_dict, balanced_threshold)




