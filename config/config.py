import configparser
import os
import inspect
from dragon_agent.exceptions import WrongConfigurationFile
from dragon_agent.utils.singleton import Singleton


class Configuration(object, metaclass=Singleton):

    def __init__(self, conf_file='config/default-config.ini'):

        self.conf_file = conf_file

        config = configparser.RawConfigParser()
        base_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))\
            .rpartition('/')[0]
        try:
            if base_folder == "":
                config.read(str(base_folder) + self.conf_file)
            else:
                config.read(str(base_folder) + '/' + self.conf_file)

            # [timeout]
            self.AGREEMENT_TIMEOUT = config.getint('timeout', 'agreement_timeout')
            self.WEAK_AGREEMENT_TIMEOUT = config.getint('timeout', 'weak_agreement_timeout')
            self.ASYNC_TIMEOUT = config.getfloat('timeout', 'async_timeout')
            self.SCHEDULING_TIME_LIMIT = config.getint('timeout', 'scheduling_time_limit')
            self.SAMPLE_FREQUENCY = config.getfloat('timeout', 'sample_frequency')

            # [neighborhood]
            self.STABLE_CONNECTIONS = config.getboolean('neighborhood', 'stable_connections')
            self.LOAD_TOPOLOGY = config.getboolean('neighborhood', 'load_topology')
            self.NEIGHBOR_PROBABILITY = config.getint('neighborhood', 'neighbor_probability')
            self.TOPOLOGY_FILE = config.get('neighborhood', 'topology_file')

            # [problem_size]
            self.SDO_NUMBER = config.getint('problem_size', 'agents_number')
            self.NODE_NUMBER = config.getint('problem_size', 'nodes_number')
            self.BUNDLE_PERCENTAGE = config.getint('problem_size', 'bundle_percentage')

            # [utility]
            self.PRIVATE_UTILITY = config.get('utility', 'private_utility')
            self.SUBMODULAR_P_UTILITY = config.getboolean('utility', 'submodular_p_utility')

            # [logging]
            self.LOG_LEVEL = config.get('logging', 'log_level')
            self.RESULTS_FOLDER = config.get('logging', 'results_folder')

            # [problem]
            self.RAP_INSTANCE = config.get('problem', 'rap_instance')

        except Exception as ex:
            raise WrongConfigurationFile(str(ex))
