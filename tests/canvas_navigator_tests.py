"""
Created on 11 Feb 2019

@author: Frank Ypma
"""
import unittest
from nipytest.canvas_navigator import CanvasNavigator
from nipyapi import config, canvas

CANVAS_CENTER: tuple = (0, 0)


class CanvasNavigatorTest(unittest.TestCase):
    nav = None
    pg_parent = None
    pg_child1 = None
    pg_child2 = None
    pg_grandchild1 = None

    proc = None
    input_port = None
    output_port = None
    controller = None
    
    @classmethod
    def setUpClass(cls):
        super(CanvasNavigatorTest, cls).setUpClass()
        print("Start of tests: preparing nifi objects")
        config.nifi_config.host = 'http://192.168.56.5:8080/nifi-api'

        root = canvas.get_process_group(canvas.get_root_pg_id(), 'id')
        # Create new process group in root
        CanvasNavigatorTest.pg_parent = canvas.create_process_group(root, "parent", CANVAS_CENTER)
        # Create new child process groups in unittest
        CanvasNavigatorTest.pg_child1 = canvas.create_process_group(
            CanvasNavigatorTest.pg_parent,
            "child1",
            CANVAS_CENTER)
        CanvasNavigatorTest.pg_child2 = canvas.create_process_group(
            CanvasNavigatorTest.pg_parent,
            "child2",
            CANVAS_CENTER)
        CanvasNavigatorTest.pg_child2_2 = canvas.create_process_group(
            CanvasNavigatorTest.pg_parent,
            "child2",
            CANVAS_CENTER)
        CanvasNavigatorTest.pg_grandchild1 = canvas.create_process_group(
            CanvasNavigatorTest.pg_child1,
            "grandchild1",
            CANVAS_CENTER)
        
        # Create other objects as well
        CanvasNavigatorTest.proc = canvas.create_processor(
            CanvasNavigatorTest.pg_parent,
            canvas.get_processor_type("GenerateFlowFile"),
            CANVAS_CENTER,
            "proc")
        CanvasNavigatorTest.input_port = canvas.create_port(
            CanvasNavigatorTest.pg_parent.component.id,
            "INPUT_PORT",
            "input_port", "STOPPED",
            CANVAS_CENTER)
        CanvasNavigatorTest.output_port = canvas.create_port(
            CanvasNavigatorTest.pg_parent.component.id,
            "OUTPUT_PORT",
            "output_port",
            "STOPPED",
            CANVAS_CENTER)
        CanvasNavigatorTest.controller = canvas.create_controller(
            CanvasNavigatorTest.pg_parent,
            canvas.list_all_controller_types()[0],
            "controller")

    @classmethod
    def tearDownClass(cls):
        print("Tests done: deleting nifi objects")
        canvas.delete_controller(canvas.get_controller(CanvasNavigatorTest.controller.component.id, 'id'), True)
        canvas.delete_process_group(CanvasNavigatorTest.pg_parent, True)

    def setUp(self):
        self.nav = CanvasNavigator()

    def test_init(self):
        print("Testing init")
        self.assertEqual(self.nav.current.component.name, "NiFi Flow", 'incorrect root component name')

    def test_cd_to_id(self):
        print("Testing cd to id")
        self.nav.cd_to_id(CanvasNavigatorTest.pg_parent.component.id)
        self.assertEqual(self.nav.current.component.name, "parent", 'did not go to correct process group id')

    def test_cd(self):
        print("Testing cd")
        self.nav.cd("parent")
        self.assertEqual(self.nav.current.component.name, "parent", 'incorrect jump to child')
        self.nav.cd("child1/grandchild1")
        self.assertEqual(self.nav.current.component.name, "grandchild1", 'incorrect jump to nested child')
        self.nav.cd("..")
        self.assertEqual(self.nav.current.component.name, "child1", 'incorrect jump to parent')
        self.nav.cd("/parent/child1/grandchild1")
        self.assertEqual(self.nav.current.component.name, "grandchild1", 'incorrect jump to nested child from root')
        # Was already called in init, but let's test again
        self.nav.cd("/")
        self.assertEqual(self.nav.current.component.name, "NiFi Flow", 'incorrect jump to root')

    def test_current_id(self):
        print("Testing current id")
        self.assertEqual(self.nav.current_id(), canvas.get_root_pg_id(), 'incorrect current id')

    def test_current_name(self):
        print("Testing current name")
        self.assertEqual(self.nav.current_name(),
                         canvas.get_process_group(canvas.get_root_pg_id(), 'id').component.name,
                         'incorrect current name')

    def test_current_parent_id(self):
        print("Testing current parent id")
        self.nav.cd("parent/child1")
        self.assertEqual(self.nav.current_parent_id(),
                         CanvasNavigatorTest.pg_parent.component.id,
                         'incorrect current parent id')

    def test_group(self):
        print("Testing group")
        self.assertEqual(self.nav.group("parent").component.id,
                         CanvasNavigatorTest.pg_parent.component.id,
                         'incorrect group')
        self.assertRaises(Exception, self.nav.group, "doesnotexist")
        self.nav.cd("parent")
        self.assertRaises(Exception, self.nav.group, "child2")

    def test_processor(self):
        print("Testing processor")
        self.nav.cd("parent")
        self.assertEqual(self.nav.processor("proc").component.id,
                         CanvasNavigatorTest.proc.component.id,
                         'incorrect processor')

    def test_input_port(self):
        print("Testing input port")
        self.nav.cd("parent")
        self.assertEqual(self.nav.input_port("input_port").component.id,
                         CanvasNavigatorTest.input_port.component.id,
                         'incorrect input port')

    def test_output_port(self):
        print("Testing output port")
        self.nav.cd("parent")
        self.assertEqual(self.nav.output_port("output_port").component.id,
                         CanvasNavigatorTest.output_port.component.id,
                         'incorrect output port')

    def test_controller_service(self):
        print("Testing controller service")
        self.nav.cd("parent")
        self.assertEqual(self.nav.controller_service("controller").component.id,
                         CanvasNavigatorTest.controller.component.id,
                         'incorrect controller service')


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
