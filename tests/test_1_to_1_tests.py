"""
Created on 27 Feb 2019

@author: Frank Ypma
"""

import unittest
from nipyapi import nifi, config, canvas
from nipytest.test_1_to_1 import Test1To1
from nipytest.models.flowfile import FlowFile
from nipytest.canvas_navigator import CanvasNavigator

CANVAS_CENTER: tuple = (0, 0)


class Test1To1Test(unittest.TestCase):
    pg_test: nifi.ProcessGroupEntity = None
    proc_start: nifi.ProcessorEntity = None
    proc_2: nifi.ProcessorEntity = None
    proc_3: nifi.ProcessorEntity = None
    proc_end: nifi.ProcessorEntity = None
    conn_1: nifi.ConnectionEntity = None
    conn_2: nifi.ConnectionEntity = None
    conn_3: nifi.ConnectionEntity = None

    test: Test1To1 = None

    @classmethod
    def setUpClass(cls):
        super(Test1To1Test, cls).setUpClass()
        print("Start of tests: preparing nifi objects")
        config.nifi_config.host = 'http://192.168.56.5:8080/nifi-api'

        flow_name = "Test1To1Test"

        nav = CanvasNavigator()
        # Delete all leftovers from previous (failed?) tests
        pgs_to_be_deleted = nav.groups(flow_name)
        for pg in pgs_to_be_deleted:
            canvas.delete_process_group(pg, force=True)
        # Create new process group in root
        Test1To1Test.pg_test = canvas.create_process_group(nav.current, flow_name, (0, 0))

        # Create simple flow to test
        Test1To1Test.proc_start = canvas.create_processor(
            Test1To1Test.pg_test,
            canvas.get_processor_type("GenerateFlowFile"),
            CANVAS_CENTER,
            "Start")
        Test1To1Test.proc_2 = canvas.create_processor(
            Test1To1Test.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "Processor 2")
        Test1To1Test.proc_3 = canvas.create_processor(
            Test1To1Test.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "Processor 3")
        Test1To1Test.proc_end = canvas.create_processor(
            Test1To1Test.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "End")
        canvas.update_processor(Test1To1Test.proc_end,
                                nifi.ProcessorConfigDTO(auto_terminated_relationships=["success", "failure"]))
        Test1To1Test.conn_1 = canvas.create_connection(Test1To1Test.proc_start, Test1To1Test.proc_2, ["success"])
        Test1To1Test.conn_2 = canvas.create_connection(Test1To1Test.proc_2, Test1To1Test.proc_3, ["success", "failure"])
        Test1To1Test.conn_3 = canvas.create_connection(Test1To1Test.proc_3, Test1To1Test.proc_end, ["success", "failure"])

        canvas.schedule_process_group(Test1To1Test.pg_test.component.id, True)

        # Create test case
        Test1To1Test.test = Test1To1("testing the tester", Test1To1Test.pg_test)

    @classmethod
    def tearDownClass(cls):
        print("Tests done: deleting nifi objects")
        canvas.delete_process_group(Test1To1Test.pg_test, force=True)

    def test_add_input(self):
        Test1To1Test.test.add_input(Test1To1Test.proc_2)

        # See if processor is in the list of inputs
        proc = [x for x in Test1To1Test.test.inputs if Test1To1Test.proc_2.component.id == x.component.id]
        assert len(proc) == 1

        # See if connection is in the list of to be removed connections
        connection = [x for x in Test1To1Test.test.connections_to_remove
                      if Test1To1Test.conn_1.component.id == x.component.id]
        assert len(connection) == 1

    def test_add_output(self):
        Test1To1Test.test.add_output(Test1To1Test.proc_3)

        # See if processor is in the list of outputs
        proc = [x for x in Test1To1Test.test.outputs if Test1To1Test.proc_3.component.id == x.component.id]
        assert len(proc) == 1

        # See if connection is in the list of to be removed connections
        connection = [x for x in Test1To1Test.test.connections_to_remove
                      if Test1To1Test.conn_3.component.id == x.component.id]
        assert len(connection) == 1

    def test_run(self):
        content_string = "This is the content of the test message"

        message = FlowFile(content_string, {"attribute1": "value1"})
        result = Test1To1Test.test.run("Processor 2", message)
        # Check result
        assert result.content == content_string
        assert result.attributes['test_input_name'] == self.proc_2.component.name
        assert result.attributes['test_output_name'] == self.proc_3.component.name
        assert int(result.attributes['test_start_time']) > 0
        assert int(result.attributes['test_end_time']) > 0
        assert int(result.attributes['test_duration']) == int(result.attributes['test_end_time']) - int(result.attributes['test_start_time'])

        # Check is test was cleaned up nicely
        # The test process group should be removed
        assert len(canvas.list_all_process_groups(Test1To1Test.pg_test.component.id)) == 1  # Only counting self
        # Check if connections were built again; there should be 3 now
        assert len(canvas.list_all_connections(Test1To1Test.pg_test.component.id, descendants=False)) == 3


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
