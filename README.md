hadoop-datanode-rebalancer
===========================

Script to rebalance data block and meta data files amongst various partitions on a single data node.

Usage
=====
python intra_datanode_balancer -p [absolute_path_to_partiton1],[absolute_path_to_partition2],...
python intra_datanode_balancer -c [path_to_hdfs-sites.xml]
