import os
import re
import sys
import shutil
from optparse import OptionParser
import xml.etree.ElementTree as ET

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
                    src_dst_dict[src_meta_path] = get_dst_path(src_partition_path, dest_partition_path, src_meta_path)
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
            shutil.move(src, dst)


def parse_hdfs_site_xml(hdfs_site_path):
    tree = ET.parse(hdfs_site_path)
    root = tree.getroot()
    dn_data_dir = None
    for p in root.findall('property'):
        name_v = next(p.iterfind('name')).text
        if name_v == 'dfs.datanode.data.dir':
            dn_data_dir = next(p.iterfind('value')).text
    return dn_data_dir.split(",")


if __name__=="__main__":
    parser = OptionParser()
    parser.add_option("-c", "--config", dest='hdfs_site', help="hdfs-site.xml's full path.")
    parser.add_option("-p", "--partitons", dest='partitions', help="The list of partitions (comma separated).")

    (options, args) = parser.parse_args()

    if options.partitions and options.hdfs_site:
        print "Can't supply both -c and -p options. Supply one or the other."
        sys.exit(1)
    if not options.partitions and not options.hdfs_site:
        print "Must supply -c or -p options."
        sys.exit(1)

    if options.hdfs_site != None:
        root_path = parse_hdfs_site_xml(options.hdfs_site)
    elif options.partitions != None:
        root_path = options.partitions.split(",")

    partition_dict = {} # {'files' : [{'name': ... , 'size':...}], 'partitions_size': {'file' : [..], 'partition_size': size }}

    for path in root_path:
        file_lst = abs_path(path)
        file_lst = size_info(file_lst)
        partition_dict[path] = { 'files': file_lst, 'partition_size': calc_total_size(file_lst), 'partition_path': path }


    balanced_threshold = sum(v['partition_size'] for k,v in partition_dict.items()) / float(len(root_path))
    print balanced_threshold
    takers_lst = get_takers(partition_dict, balanced_threshold)
    givers_lst = get_givers(partition_dict, balanced_threshold)


    manifest_dict = gen_rebalance_manifest(givers_lst, takers_lst, balanced_threshold, partition_dict)
    exec_rebalance_manifest(manifest_dict)




