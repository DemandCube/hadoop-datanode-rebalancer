import os
from intra_datanode_balancer import (
    abs_path,
    size_info,
    calc_total_size,
    get_takers,
    get_givers,
    gen_rebalance_manifest,
)



class TestIntraDatanodeBalancer:
    ROOT_PATH_TMP_0 = "%s/tests/data/hadoop0"
    ROOT_PATH_TMP_1 = "%s/tests/data/hadoop1"
    PREFIX_TMP = '/path/to/partition/%s/'

    def setUp(self):
        self.root_path_0 = self.ROOT_PATH_TMP_0 % os.getcwd()
        self.root_path_1 = self.ROOT_PATH_TMP_1 % os.getcwd()


    def test_abs_path(self):
        r_lst = abs_path(self.root_path_1)
        assert len(r_lst) > 0, "abs_path didn't return any block data file and meta data file information."

        for x in r_lst:
            assert 'metapath' in x, "The metapath key is missing."
            assert 'path' in x, "The path key is missing."
            assert 'size' in x, "The size key is missing."
            assert x['size'] == None
            assert "_".join(x['metapath'].split('.meta')[0].split('_')[:-1]) == x['path']


    def test_size_info(self):
        file_lst = abs_path(self.root_path_1)
        r_lst = size_info(file_lst)

        for x in r_lst:
            assert 'metapath' in x, "The metapath key is missing."
            assert 'path' in x, "The path key is missing."
            assert 'size' in x, "The size key is missing."
            assert x['size'] != None
            assert x['size'] > 0
            assert "_".join(x['metapath'].split('.meta')[0].split('_')[:-1]) == x['path']
            assert os.path.getsize(x['metapath']) + os.path.getsize(x['path']) == x['size']


    def test_calc_total_size(self):
        file_lst = abs_path(self.root_path_1)
        file_lst = size_info(file_lst)

        sum_r = calc_total_size(file_lst)
        assert sum_r > 0


    def generate_mock_partition_files(self, root):
        """
        root is := hadoop0, hadoop1, or hadoop2
        """
        prefix = self.PREFIX_TMP % root

        if root == 'hadoop0':
            r = [{
                'path' : prefix + 'hadoop0_data_block0',
                'metapath' : prefix + 'hadoop0_data_block0_000.meta',
                'size': 111
                },
                {
                'path' : prefix + 'hadoop0_data_block1',
                'metapath' : prefix + 'hadoop0_data_block1_111.meta',
                'size': 222
                },
                {
                'path' : prefix + 'hadoop0_data_block2',
                'metapath' : prefix + 'hadoop0_data_block2_222.meta',
                'size': 10000
                }]
        elif root == 'hadoop1':
            r = [{
                'path' : prefix + 'hadoop1_data_block0',
                'metapath' : prefix + 'hadoop0_data_block0_000.meta',
                'size': 111
                },
                {
                'path' : prefix + 'hadoop1_data_block1',
                'metapath' : prefix + 'hadoop1_data_block1_111.meta',
                'size': 222
                }]
        elif root == 'hadoop2':
            r = []
            pass

        return r

    def generate_partition_dict(self):
        partition_dict = {}

        hadoop0_files = self.generate_mock_partition_files('hadoop0')
        partition_dict['hadoop0']= {
            'files' : hadoop0_files,
            'partition_size': calc_total_size(hadoop0_files),
            'partition_path': self.PREFIX_TMP % 'hadoop0'
        }

        hadoop1_files = self.generate_mock_partition_files('hadoop1')
        partition_dict['hadoop1']= {
            'files' : hadoop0_files,
            'partition_size': calc_total_size(hadoop1_files),
            'partition_path': self.PREFIX_TMP % 'hadoop1'
        }

        hadoop2_files = self.generate_mock_partition_files('hadoop2')
        partition_dict['hadoop2']= {
            'files' : hadoop0_files,
            'partition_size': calc_total_size(hadoop2_files),
            'partition_path': self.PREFIX_TMP % 'hadoop2'
        }
        return partition_dict


    def test_get_takers(self):
        partition_dict = self.generate_partition_dict()
        takers_lst = get_takers(partition_dict, 333)
        assert 'hadoop2' in takers_lst
        assert 'hadoop0' not in takers_lst
        assert 'hadoop1' not in takers_lst

        takers_lst = get_takers(partition_dict, 500)
        assert 'hadoop2' in takers_lst
        assert 'hadoop1' in takers_lst
        assert 'hadoop0' not in takers_lst

    def test_get_givers(self):
        partition_dict = self.generate_partition_dict()
        givers_lst = get_givers(partition_dict, 333)
        assert 'hadoop0' in givers_lst
        assert 'hadoop1' not in givers_lst
        assert 'hadoop2' not in givers_lst

        partition_dict = self.generate_partition_dict()
        givers_lst = get_givers(partition_dict, 300)
        assert 'hadoop0' in givers_lst
        assert 'hadoop1' in givers_lst
        assert 'hadoop2' not in givers_lst

    def test_gen_rebalance_manifest(self):
        partition_dict = self.generate_partition_dict()
        balanced_threshold = 333
        givers_lst = get_givers(partition_dict, balanced_threshold)
        takers_lst = get_takers(partition_dict, balanced_threshold)

        manifest_dict = gen_rebalance_manifest(givers_lst, takers_lst, balanced_threshold, partition_dict)
        assert '/path/to/partition/hadoop0/hadoop0_data_block0' in manifest_dict
        assert manifest_dict['/path/to/partition/hadoop0/hadoop0_data_block0'] == '/path/to/partition/hadoop2/hadoop0_data_block0'

        assert '/path/to/partition/hadoop0/hadoop0_data_block0_000.meta' in manifest_dict
        assert manifest_dict['/path/to/partition/hadoop0/hadoop0_data_block0_000.meta'] == '/path/to/partition/hadoop2/hadoop0_data_block0_000.meta'


