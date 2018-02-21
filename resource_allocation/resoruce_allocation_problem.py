import pprint

import math

import sys


class ResourceAllocationProblem:

    def __init__(self, sdos=None, services=None, functions=None, resources=None, nodes=None,
                 consumption=None, available_resources=None, implementation=None):
        """

        :param sdos: sdos list, index i of the linear formulation
        :param services: services list, index m of the linear formulation
        :param functions: functions list, index j of the linear formulation
        :param resources: physical resources list, index k of the linear formulation
        :param nodes: physical nodes list, index n of the linear formulation
        :param consumption: consumption matrix, stores costs in terms of each resource k for function j
        :param available_resources: for eache node n, stores the total amount of each resource k available
        :param implementation: dict that lists, for each service, all possible functions that can implement it

        :type sdos: list of str
        :type services: list of str
        :type functions: list of str
        :type resources: list of str
        :type consumption: dict[str, dict[str, int]]
        :type available_resources: dict[str, dict[str, int]]
        :type implementation: dict[str, (list of str)]
        """

        # indexes
        self.sdos = sdos
        self.functions = functions
        self.services = services
        self.resources = resources
        self.nodes = nodes

        # problem instance data
        self.consumption = consumption
        self.available_resources = available_resources
        self.implementation = implementation

    def parse_dict(self, rap_dict):

        self.sdos = rap_dict["sdos"]
        self.functions = rap_dict["functions"]
        self.services = rap_dict["services"]
        self.resources = rap_dict["resources"]
        self.nodes = rap_dict["nodes"]
        self.consumption = rap_dict["consumption"]
        self.available_resources = rap_dict["available_resources"]
        self.implementation = rap_dict["implementation"]

    def to_dict(self):

        rap_dict = dict()
        rap_dict["sdos"] = self.sdos
        rap_dict["functions"] = self.functions
        rap_dict["services"] = self.services
        rap_dict["resources"] = self.resources
        rap_dict["nodes"] = self.nodes
        rap_dict["consumption"] = self.consumption
        rap_dict["available_resources"] = self.available_resources
        rap_dict["implementation"] = self.implementation
        return rap_dict

    def check_node_bounded(self, node_assignment_dict, node):
        """
        Checks infrastructure-bounded property of the given assignment for the given node
        :param dict[str, union[int, dict]] node_assignment_dict: the assignment_dict to check,
        is a dict {sdo:{'bid':int,'consumption':dict}}
        :param str node: the node where the assignment should be bounded
        :return: True if is bounded
        """
        node_assignment_dict_consumption = self.get_node_assignment_dict_consumption(node_assignment_dict)
        for resource in self.resources:
            if node_assignment_dict_consumption[resource] > self.available_resources[node][resource]:
                return False
        return True

    def check_infrastructure_bound(self, assignment_dict):
        """
        Checks infrastructure-bounded property of the given assignment
        :param dict[str, dict[str, union[int, dict]]] assignment_dict: the assignment_dict to check,
        is a dict {node:{sdo:{'bid':int,'consumption':dict}}}
        :return: true if current assignment_dict is infrastructure-bounded
        """
        for node in assignment_dict:
            if not self.check_node_bounded(assignment_dict[node], node):
                return False
        return True

    def check_custom_bound(self, assignment_dict, bounds):
        """
        Checks if the given assignment fit the resource amount given as parameter for each node
        :param dict[str, dict[str, union[int, dict]]] assignment_dict: the assignment_dict to check,
        is a dict {'node':{sdo:{'bid':int,'consumption':dict}}}
        :param dict[str, dict[str, int]] bounds: resources to fit for each node
        :return: true if current assignment_dict is infrastructure-bounded
        """
        for node in set(assignment_dict.keys()):
            if bounds[node] is None:
                return False
            if not self.check_custom_node_bound(assignment_dict[node], bounds[node]):
                return False
        return True

    def check_custom_node_bound(self, node_assignment_dict, bound):
        """

        :param node_assignment_dict:
        :param bound:
        :return:
        """
        node_assignment_dict_consumption = self.get_node_assignment_dict_consumption(node_assignment_dict)
        for resource in self.resources:
            if node_assignment_dict_consumption[resource] > bound[resource]:
                return False
        return True

    def get_total_resources_amount(self):
        """

        :return:
        """
        total_resources = {resource: 0 for resource in self.resources}

        for node in self.nodes:
            total_resources = self.sum_resources(total_resources, self.available_resources[node])
        return total_resources

    def get_residual_resources(self, assignment_dict):
        """
        Get residual resources on node given node_assignment_dict
        :param dict[str, union[int, dict]] assignment_dict: the given assignment_dict,
        is a dict {sdo:{'bid':int,'consumption':dict}}
        :return: the residual resources for each node
        """
        residual_resources = dict()
        for node in self.nodes:
            if node not in assignment_dict:
                residual_resources[node] = self.available_resources[node]
            else:
                residual_resources[node] = self.get_residual_resources_on_node(assignment_dict[node], node)
        return residual_resources

    def get_residual_resources_on_node(self, node_assignment_dict, node):
        """
        Get residual resources on node given node_assignment_dict
        :param dict[str, dict[str, union[int, dict]]] node_assignment_dict: the given assignment_dict,
        is a dict {sdo:{'bid':int,'consumption':dict}}
        :param str node: the node
        :return: the residual resources on the given node
        """
        assignment_dict_consumption = self.get_node_assignment_dict_consumption(node_assignment_dict)
        residual_res = dict()
        for resource in self.resources:
            if assignment_dict_consumption[resource] > self.available_resources[node][resource]:
                return None
            residual_res[resource] = self.available_resources[node][resource] - assignment_dict_consumption[resource]
        return residual_res

    def check_waste_freedom(self):
        """
        Checks waste-free property of the current assignment_dict
        :return: true if current assignment_dict is waste-free
        """
        pass

    def get_node_assignment_dict_consumption(self, node_assignment_dict):
        """

        :param dict[str, union[int, dict]] node_assignment_dict:
        :return dict[str, int]:
        """
        assignment_dict_consumption = {r: 0 for r in self.resources}
        for sdo in node_assignment_dict:
            if 'consumption' in node_assignment_dict[sdo]:
                assignment_dict_consumption = self.sum_resources(assignment_dict_consumption,
                                                                 node_assignment_dict[sdo]['consumption'])
        return assignment_dict_consumption

    def get_function_resource_consumption(self, function):
        """
        Return a dict with all the consumption for the given function
        :param function: the function of which we want to calculate the consumption
        :return:
        """
        return self.consumption[function]

    def get_bundle_resource_consumption(self, functions):
        """
        Return a dict with all the consumption for the given bundle of function
        :param functions: list of functions
        :return:
        """
        # init total consumption for each resource
        total_consumption = dict()
        for resource in self.resources:
            total_consumption[resource] = 0
        # add each function consumption to total
        for function in functions:
            total_consumption = self.sum_resources(total_consumption, self.consumption[function])
        return total_consumption

    def sum_resources(self, resources_a, resources_b):
        """

        :param resources_a:
        :param resources_b:
        :return:
        """
        sum_resources = dict()
        for resource in self.resources:
            sum_resources[resource] = resources_a[resource] + resources_b[resource]
        return sum_resources

    def sub_resources(self, resources_a, resources_b):
        """

        :param resources_a:
        :param resources_b:
        :return:
        """
        sub_resources = dict()
        for resource in self.resources:
            sub_resources[resource] = resources_a[resource] - resources_b[resource]
        return sub_resources

    def check_equals(self, resources_a, resources_b):
        """

        :param resources_a:
        :param resources_b:
        :return:
        """
        for resource in self.resources:
            if resources_a[resource] != resources_b[resource]:
                return False
        return True

    @staticmethod
    def get_sdo_utility_node_assignment(assignment_dict, sdo):
        """

        :param dict[str, dict[str, union[int, dict]]] assignment_dict: the assignment_dict,
        :param sdo:
        :return:
        """
        overall_utility = 0
        for node in assignment_dict:
            overall_utility += ResourceAllocationProblem.get_sdo_utility_for_node_assignment(assignment_dict[node], sdo)
        return overall_utility

    @staticmethod
    def get_sdo_utility_for_node_assignment(node_assignment_dict, sdo):
        """

        :param node_assignment_dict:
        :param sdo:
        :return:
        """
        sdo_utility = 0
        for function, utility in node_assignment_dict[sdo]:
            sdo_utility += utility
        return sdo_utility

    '''
    def init_assignment_dict(self):
        """

        :return:
        """
        assignment_dict = dict()
        for sdo in self.sdos:
            assignment_dict[sdo] = list()
        return assignment_dict
    '''

    def check_function_implements_service(self, service, function):
        """

        :param service:
        :param function:
        :return:
        """
        return function in self.implementation[service]

    '''
    def get_implementations(self):
        """

        :return:
        """
        return self.implementation
    '''

    def get_implementations_for_service(self, service):
        """

        :param service:
        :return:
        """
        return self.implementation[service]

    def norm(self, node, resources):
        """

        :param node:
        :param resources:
        :return:
        """
        quadratic_values = list()
        for resource in self.resources:

            consumption = resources[resource]*self.resource_scalar(resource, node)
            quadratic_value = consumption**2
            quadratic_values.append(quadratic_value)

        weighted_quadratic_norm = math.sqrt(sum(quadratic_values))
        return weighted_quadratic_norm

    def resource_scalar(self, resource, node=None):
        """

        :param resource:
        :param node:
        :return:
        """
        if node is None:
            total_resources_amount = self.get_total_resources_amount()
        else:
            total_resources_amount = self.available_resources[node]
        avg = sum(total_resources_amount.values())/len(self.resources)
        return avg/total_resources_amount[resource]

    def __str__(self):
        return "************************* RAP INSTANCE ************************\n" + "sdos: " + str(self.sdos) + "\n " \
               "services: " + str(self.services) + "\n" \
               "functions: " + str(self.functions) + "\n" \
               "resources: " + str(self.resources) + "\n" \
               "nodes: " + str(self.nodes) + "\n" \
               "available_resources: " + pprint.pformat(self.available_resources) + "\n" \
               "consumption: \n" + pprint.pformat(self.consumption, compact=True) + "\n" \
               "implementation: \n" + pprint.pformat(self.implementation, compact=True) + "\n" \
               "***************************************************************\n"

