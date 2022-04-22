'''
WIP: Experiment to test the speed of computing min() over a range of numbers
inspired by exp_mariska.py
'''

from datetime import datetime

from rados_deploy import Designation

from experimenter.internal.experiment.execution.execution_interface import ExecutionInterface
import experimenter.internal.experiment.execution.functionstore.data_general as data_general
import experimenter.internal.experiment.execution.functionstore.distribution_general as distribution_general
import experimenter.internal.experiment.execution.functionstore.experiment_general as experiment_general
import experimenter.internal.experiment.execution.functionstore.spark as spark
import experimenter.internal.experiment.execution.functionstore.rados_ceph as rados_ceph

from experimenter.internal.experiment.interface import ExperimentInterface
from experimenter.internal.experiment.config import ExperimentConfigurationBuilder, ExperimentConfiguration, NodeConfiguration, CephConfiguration

import utils.fs as fs
import utils.location as loc
from utils.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so we can find it when loading.'''
    return CephExperiment()

        
def get_node_configuration():
    return NodeConfiguration(4, CephConfiguration([
            [Designation.OSD, Designation.MON, Designation.MGR], 
            [Designation.OSD, Designation.MON, Designation.MGR],
            [Designation.OSD, Designation.MON, Designation.MDS],
            [Designation.OSD, Designation.MON, Designation.MDS]]))


class CephExperiment(ExperimentInterface):
    def __init__(self):
        super(CephExperiment, self).__init__()

    def get_executions(self):
        query = 'SELECT MIN(value) FROM table'
        bathsize = 4096
        stripe = 16
        timestamp = datetime.now().isoformat()
        datafiles = ['numbers_100k.parquet', 'numbers_1m.parquet']
        copy_multiplier, link_multiplier = (1, 1) 

        configs = []

        for datafile in datafiles:
            result_dirname = str(datafile)
            configbuilder = ExperimentConfigurationBuilder()
            configbuilder.set('runs', 1)
            configbuilder.set('batchsize', bathsize)
            configbuilder.set('node_config', get_node_configuration())
            configbuilder.set('stripe', stripe)
            configbuilder.set('remote_result_dir', fs.join('~', 'results', 'exp_nums', str(timestamp), result_dirname))
            configbuilder.set('result_dir', fs.join(loc.result_dir(), 'exp_nums', str(timestamp), result_dirname))
            configbuilder.set('data_path', fs.join(loc.data_generation_dir(), datafile))
            configbuilder.set('data_query', '"{}"'.format(query))
            configbuilder.set('copy_multiplier', copy_multiplier)
            configbuilder.set('link_multiplier', link_multiplier)
            config = configbuilder.build()
            configs.append(config)

        for idx, config in enumerate(configs):
            executionInterface = ExecutionInterface(config)
            executionInterface.register('distribute_func', distribution_general.distribute_automatic)
            experiment_general.register_default_experiment_function(executionInterface, idx, len(configs))
            experiment_general.register_default_result_fetch_function(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_deploy_data(executionInterface, idx, len(configs))
            spark.register_spark_functions(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_functions(executionInterface, idx, len(configs))
            yield executionInterface



