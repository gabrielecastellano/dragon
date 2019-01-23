import hashlib
import json
import time


class NeighborhoodDetector:

    def __init__(self, sdos, base_sdo, load_neighborhood, neighbor_probability=None, topology_file=None,
                 stable_connections=False):
        """

        :param list of str sdos:
        :param str base_sdo:
        :param bool load_neighborhood:
        :param int neighbor_probability:
        :param str topology_file:
        :param bool stable_connections: if True, neighborhood is fixed
        """
        self.sdos = sdos
        self.base_sdo = base_sdo
        self.neighbor_probability = neighbor_probability
        self.stable_connections = stable_connections
        if load_neighborhood:
            with open(topology_file, "r") as f:
                topology = json.loads(f.read())
            self.neighborhood = [sdo for sdo in self.sdos
                                 if sdo != self.base_sdo
                                 and self._topology_check_neighbors(self.base_sdo, sdo, topology)]
        else:
            self.neighborhood = [sdo for sdo in self.sdos
                                 if sdo != self.base_sdo
                                 and self._pseudo_random_check_neighbors(self.base_sdo, sdo)]

    def get_neighborhood(self):
        """
        Return the list of static neighbors.
        :return:
        """
        return self.neighborhood

    def get_current_neighborhood(self):
        """
        Return the list of current connected neighbors.
        :return:
        """
        if self.stable_connections:
            return self.neighborhood
        else:
            return [sdo for sdo in self.neighborhood if self._pseudo_random_check_connection(self.base_sdo, sdo)]

    def _pseudo_random_check_neighbors(self, sdo1, sdo2):
        """

        :param sdo1:
        :param sdo2:
        :return:
        """
        # establish if the given sdos are physically one-hop-neighbors
        sdos_digest = int(hashlib.sha256(("1"+str(sorted([sdo1, sdo2]))).encode()).hexdigest(), 16)
        return int(str(sdos_digest)[-2:]) < self.neighbor_probability
        # return int(bin(sdos_digest)[-1:]) == 0

    def _topology_check_neighbors(self, sdo1, sdo2, topology):
        """

        :param sdo1:
        :param sdo2:
        :param topology:
        :return:
        """
        # establish if the given sdos are physically one-hop-neighbors
        return sdo2 in self.sdos and sdo2 in topology[sdo1]

    def _pseudo_random_check_connection(self, sdo1, sdo2):
        """

        :param sdo1:
        :param sdo2:
        :return bool:
        """
        if sdo2 not in self.neighborhood:
            return False

        if not self.stable_connections:
            # establish if at the given time the two neighbors has connectivity (prob 0.75)
            # may change every 10 seconds
            time_token = int(time.time()/10)
            sdos_time_digest = int(hashlib.sha256((str(sorted([sdo1, sdo2]))+str(time_token)).encode()).hexdigest(), 16)
            return int(bin(sdos_time_digest)[-2:]) != 0

        return True

    def _check_neighbors(self, sdo1, sdo2):
        """

        :param sdo1:
        :param sdo2:
        :return:
        """
        pass
