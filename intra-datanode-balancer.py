import os
import re
import shutil

# TODO: hardcoded for now
ROOT_PATH = [
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop/",
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop1/",
    "/Users/lchen/Documents/workspace/test_partition_data/hadoop2/",
]
META_REGEX = re.compile(r".*?\.meta$")

def abs_path(root):
    """
    input: root: absolute path. Need to be the same as the hdfs-site.xml
    output: [ {path: filename, size: None}, ...]

    output: [ {path: filename, metapath: metafilename, size: None}, ...]
            size := size(filename) + size(metafilename)
    Assumption: the data block file and the meta file come in pair.
    """
    r = []
    # grab only the meta file paths
    for dirpath, dirnames, filenames in os.walk(root):
        if len(filenames) > 0:
            r += [ dict([("metapath", "%s/%s" % (dirpath, f)), ("size", None)])  for f in filenames if META_REGEX.search(f) != None ]

    # insert the data block paths
    for entry in r:
        entry['path'] = "_".join(entry['metapath'].split("_")[:-1])

    return r


def size_info(file_list):
    for entry in file_list:
        entry['size'] = os.path.getsize(entry['path']) + os.path.getsize(entry['metapath'])
    return sorted(file_list, key=lambda entry: entry['size'], reverse=True) #sort by size


def calc_total_size(file_list):
    return sum(entry['size'] for entry in file_list)


def get_takers(partition_dict, balanced_threshold):
    """
    return a list of partitions that are below the balanced threshold.

    """
    return [k for k,v in partition_dict.items() if v['partition_size'] < balanced_threshold]


def get_givers(partition_dict, balanced_threshold):
    """
    Return a list of partitions that are above the balanced threshold.
    """
    return [k for k,v in partition_dict.items() if v['partition_size'] > balanced_threshold]


def move_file_generator(give_lst, balanced_threshold):
    # best fit
    for g in give_lst:
        partition_entry = partition_dict[g]
        partition_size = partition_entry['partition_size']
        partition_path = partition_entry['partition_path']

        file_entry_lst = partition_entry['files']

        delta_size = partition_size - balanced_threshold
        taken_size = 0
        for idx, f_entry in enumerate(file_entry_lst):
            f_entry_size = f_entry['size']
            if (taken_size + f_entry_size) < delta_size:
                print "giver: balanced_threshold : %f | total_partition_size: %f" % (balanced_threshold, partition_size - taken_size)
                taken_size += f_entry_size
                del file_entry_lst[idx]
                yield (partition_path, f_entry['path'], f_entry['metapath'], f_entry_size)
            else:
                print "skip"


def get_dst_path(src_partition_path, dst_partition_path, src_name):
    """
    src_name: full file path
    replace the src_partition portion in src_name with dst_partition_path
    """
    return src_name.replace(src_partition_path, dst_partition_path)


def gen_rebalance_manifest(givers_lst, takers_lst, balanced_threshold, partition_dict):
    src_dst_dict = {} # key: src path, value dst path

    for t in takers_lst:
        g = move_file_generator(givers_lst, balanced_threshold)
        print "dealing with another taker"
        partition_entry = partition_dict[t]
        partition_size = partition_entry['partition_size']
        dest_partition_path = partition_entry['partition_path']

        delta_size = balanced_threshold - partition_size
        stolen_size = 0

        while stolen_size < delta_size:
            try:
                print "taker > balanced_threshold : %f | total_partition_size: %f" % (balanced_threshold, partition_size + stolen_size)
                src_partition_path, src_path, src_meta_path, f_size = next(g)
                if (stolen_size + f_size) < delta_size:
                    stolen_size += f_size
                    src_dst_dict[src_path] = get_dst_path(src_partition_path, dest_partition_path, src_path)
            except StopIteration:
                break

    return src_dst_dict


def exec_rebalance_manifest(manifest_dict):
    for src, dst in manifest_dict.items():
        print "moving %s to %s" % (src, dst)
        try:
            shutil.move(src, dst)
        except IOError:
            # need to create the directory
            target_dir = "/".join(dst.split("/")[:-1])
            os.makedirs(target_dir)


if __name__=="__main__":
    partition_dict = {} # {'files' : [{'name': ... , 'size':...}], 'partitions_size': {'file' : [..], 'partition_size': size }}

    for path in ROOT_PATH:
        file_lst = abs_path(path)
        file_lst = size_info(file_lst)
        partition_dict[path] = { 'files': file_lst, 'partition_size': calc_total_size(file_lst), 'partition_path': path }


    balanced_threshold = sum(v['partition_size'] for k,v in partition_dict.items()) / float(len(ROOT_PATH))
    print balanced_threshold
    takers_lst = get_takers(partition_dict, balanced_threshold)
    givers_lst = get_givers(partition_dict, balanced_threshold)
    import pdb; pdb.set_trace() #xxx


    manifest_dict = gen_rebalance_manifest(givers_lst, takers_lst, balanced_threshold, partition_dict)
    exec_rebalance_manifest(manifest_dict)




