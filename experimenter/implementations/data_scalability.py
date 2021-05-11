from internal.experiment.interface import ExperimentInterface
import utils.location as loc
from utils.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so MetaSpark can find it'''
    return DataScalabilityExperiment


class DataScalabilityExperiment(ExperimentInterface):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    def get_configs(self):
        '''Get experiment configs.
        Returns:
            list(internal.experiment.ExperimentConfiguration), containing all different setups we want to experiment with.'''
        stripe = 64 # One file should have stripe size of 64MB
        data_multipliers = [1024, 2*1024, 4*1024, 8*1024, 16*1024] #Total data sizes: 64, 128, 256, 512, 1024 GB
        for x in data_multipliers:
            conf = ExperimentConfiguration()
            conf.stripe = stripe
            conf.data_multiplier = x
            yield conf

    def on_start(self, config, nodes_spark, nodes_ceph, num_experiment, amount_experiments):
        '''Function hook, called when we start an experiment. This function is called after resources have been allocated.
        Args:
            config (internal.experiment.ExperimentConfiguration): Current experiment configuration.
            nodes_spark (tuple(metareserve.Node, list(metareserve.Node))): Spark master, Spark worker nodes.
            nodes_ceph (tuple(metareserve.Node, list(metareserve.Node))): Ceph admin, Ceph other nodes. Note: The extra_info attribute of each node tells what designations it hosts with the "designations" key. 
            num_experiment (int): Current experiment run number (e.g. if we now run 4/10, value will be 4).
            amount_experiments (int): Total amount of experiments to run. (e.g. if we now run 4/10, value will be 10).

        Returns:
            `True` if we want to proceed with the experiment. `False` if we want to cancel it.'''
        return True

    def on_stop(self):
        return False