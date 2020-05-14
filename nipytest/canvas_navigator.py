# coding: utf-8

"""
Created on 9 Feb 2019

@author: Frank Ypma
"""
import logging
from nipyapi import canvas, nifi


# Separator used for "paths"
SEPARATOR: str = "/"


class CanvasNavigator:
    """
    Navigtor module to ascend and descend the processor group tree on the nifi canvas
    Works with some standard linux commands as cd and ls
    Can return certain objects inside the current process group
    """

    def __init__(self):
        self.logger = logging.getLogger('CanvasNavigator')
        self.logger.setLevel(logging.DEBUG)
        self.current = nifi.ProcessGroupsApi().get_process_group(canvas.get_root_pg_id())

    # Jump directly into process group id
    def cd_to_id(self, pg_id):
        self.current = nifi.ProcessGroupsApi().get_process_group(pg_id)

    # Print child process groups
    def ls(self):
        print("Showing contents of " + self.current_id() + " - " + self.current_name())
        for child in self.__child_groups():
            print("\t" + child.component.id + " - PG - " + child.component.name)
        for child in self.__child_procossors():
            print("\t" + child.component.id + " - Pr - " + child.component.name)
        for child in self.__child_input_ports():
            print("\t" + child.component.id + " - IP - " + child.component.name)
        for child in self.__child_output_ports():
            print("\t" + child.component.id + " - OP - " + child.component.name)

    # Returns children process groups
    def __child_groups(self):
        return nifi.ProcessGroupsApi().get_process_groups(self.current_id()).process_groups

    # Returns children processors
    def __child_processors(self):
        return nifi.ProcessGroupsApi().get_processors(self.current_id()).processors

    # Returns children input ports
    def __child_input_ports(self):
        return nifi.ProcessGroupsApi().get_input_ports(self.current_id()).input_ports

    # Returns children output ports
    def __child_output_ports(self):
        return nifi.ProcessGroupsApi().get_output_ports(self.current_id()).output_ports

    # Returns children controller services
    def __child_controller_services(self):
        controller_services = canvas.list_all_controllers(self.current_id(), False)
        return [cs for cs in controller_services if cs.parent_group_id == self.current_id()]

    # Change current process group to child with name
    def cd(self, path):
        if path[:1] == SEPARATOR:
            self.cd_to_id(canvas.get_root_pg_id())  # Go to root
            path = path[1:]  # Remove top level SEPARATOR; traverse path relative from root

        if len(path) > 0:
            if path == "..":
                if self.current_id() == canvas.get_root_pg_id():
                    return
                return self.cd_to_id(self.current_parent_id())

            child_pg_names = path.split(SEPARATOR)
            for child_pg_name in child_pg_names:
                self.__cd_to_child(child_pg_name)

    # Change current process group to child with name
    def __cd_to_child(self, child_pg_name):
        pg = self.group(child_pg_name)
        if pg is not None:
            self.current = pg

    # Return current process group id
    def current_id(self):
        return self.current.component.id

    # Return current process group name
    def current_name(self):
        return self.current.component.name

    # Return current process group parent id
    def current_parent_id(self):
        return self.current.component.parent_group_id

    # Return children of certain type with certain name
    def __children(self, child_type, name):
        children = None

        if child_type == "PROCESS_GROUP":
            children = self.__child_groups()
        elif child_type == "PROCESSOR":
            children = self.__child_processors()
        elif child_type == "INPUT_PORT":
            children = self.__child_input_ports()
        elif child_type == "OUTPUT_PORT":
            children = self.__child_output_ports()
        elif child_type == "CONTROLLER_SERVICE":
            children = self.__child_controller_services()

        if children is None:
            raise Exception("Search for type "+child_type+"not supported")

        return [child for child in children if child.component.name == name]

    # Return child of certain type with certain name
    def __child(self, child_type, name):
        children_matched = self.__children(child_type, name)
        if len(children_matched) == 0:
            raise Exception(child_type+" "+name+" not found in "+self.current.component.name)
        elif len(children_matched) > 1:
            raise Exception("Found multiple "+child_type+" with name "+name+" in "+self.current.component.name)

        # Return single matching child
        return children_matched[0]

    # Return child process group. Throws error if multiple are found
    def group(self, name):
        return self.__child("PROCESS_GROUP", name)

    # Return child process groups
    def groups(self, name):
        return self.__children("PROCESS_GROUP", name)

    # Return child processor
    def processor(self, name):
        return self.__child("PROCESSOR", name)

    # Return child processor
    def input_port(self, name):
        return self.__child("INPUT_PORT", name)

    # Return child processor
    def output_port(self, name):
        return self.__child("OUTPUT_PORT", name)

    # Return controller service
    def controller_service(self, name):
        return self.__child("CONTROLLER_SERVICE", name)
