"""
Created on 27 Feb 2019

@author: Frank Ypma
"""

import unittest
from nipyapi import nifi, config, canvas
from nipytest.test_1_to_n import Test1ToN
from nipytest.models.flowfile import FlowFile
from nipytest.canvas_navigator import CanvasNavigator

CANVAS_CENTER: tuple = (0, 0)


class Test1ToNTest(unittest.TestCase):
    pg_test: nifi.ProcessGroupEntity = None
    proc_start: nifi.ProcessorEntity = None
    proc_2: nifi.ProcessorEntity = None
    proc_3: nifi.ProcessorEntity = None
    proc_end_1: nifi.ProcessorEntity = None
    proc_end_2: nifi.ProcessorEntity = None
    conn_1: nifi.ConnectionEntity = None
    conn_2: nifi.ConnectionEntity = None
    conn_3: nifi.ConnectionEntity = None
    conn_4: nifi.ConnectionEntity = None

    @classmethod
    def setUpClass(cls):
        super(Test1ToNTest, cls).setUpClass()
        print("Start of tests: preparing nifi objects")
        config.nifi_config.host = 'http://192.168.56.5:8080/nifi-api'

        flow_name = "Test1ToNTest"

        nav = CanvasNavigator()
        # Delete all leftovers from previous (failed?) tests
        pgs_to_be_deleted = nav.groups(flow_name)
        for pg in pgs_to_be_deleted:
            canvas.delete_process_group(pg, force=True)
        # Create new process group in root
        Test1ToNTest.pg_test = canvas.create_process_group(nav.current, flow_name, (0, 0))

        # Create simple flow to test
        Test1ToNTest.proc_start = canvas.create_processor(
            Test1ToNTest.pg_test,
            canvas.get_processor_type("GenerateFlowFile"),
            CANVAS_CENTER,
            "Start")
        Test1ToNTest.proc_2 = canvas.create_processor(
            Test1ToNTest.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "Processor 2")
        Test1ToNTest.proc_3 = canvas.create_processor(
            Test1ToNTest.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "Processor 3")
        Test1ToNTest.proc_end_1 = canvas.create_processor(
            Test1ToNTest.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "End 1")
        Test1ToNTest.proc_end_2 = canvas.create_processor(
            Test1ToNTest.pg_test,
            canvas.get_processor_type("DebugFlow"),
            CANVAS_CENTER,
            "End 2")
        canvas.update_processor(Test1ToNTest.proc_end_1,
                                nifi.ProcessorConfigDTO(auto_terminated_relationships=["success", "failure"]))
        canvas.update_processor(Test1ToNTest.proc_end_2,
                                nifi.ProcessorConfigDTO(auto_terminated_relationships=["success", "failure"]))
        Test1ToNTest.conn_1 = canvas.create_connection(Test1ToNTest.proc_start, Test1ToNTest.proc_2, ["success"])
        Test1ToNTest.conn_2 = canvas.create_connection(Test1ToNTest.proc_2, Test1ToNTest.proc_3, ["success", "failure"])
        Test1ToNTest.conn_3 = canvas.create_connection(Test1ToNTest.proc_3, Test1ToNTest.proc_end_1, ["success", "failure"])
        Test1ToNTest.conn_4 = canvas.create_connection(Test1ToNTest.proc_3, Test1ToNTest.proc_end_2, ["success", "failure"])

        canvas.schedule_process_group(Test1ToNTest.pg_test.component.id, scheduled=True)

    @classmethod
    def tearDownClass(cls):
        print("Tests done: deleting nifi objects")
        canvas.schedule_process_group(Test1ToNTest.pg_test.component.id, scheduled=False)
        # canvas.purge_process_group(Test1ToNTest.pg_test, stop=True)
        # canvas.delete_process_group(Test1ToNTest.pg_test, force=True)

    def test_add_input(self):
        test = Test1ToN("Test add input", Test1ToNTest.pg_test)
        test.add_input(Test1ToNTest.proc_2)

        # See if processor is in the list of inputs
        proc = [x for x in test.inputs if Test1ToNTest.proc_2.component.id == x.component.id]
        assert len(proc) == 1

        # See if connection is in the list of to be removed connections
        connection = [x for x in test.connections_to_remove
                      if Test1ToNTest.conn_1.component.id == x.component.id]
        assert len(connection) == 1

    def test_add_output(self):
        test = Test1ToN("Test add output", Test1ToNTest.pg_test)
        test.add_output(Test1ToNTest.proc_3)

        # See if processor is in the list of outputs
        proc = [x for x in test.outputs if Test1ToNTest.proc_3.component.id == x.component.id]
        assert len(proc) == 1

        # See if connection is in the list of to be removed connections
        connection = [x for x in test.connections_to_remove
                      if Test1ToNTest.conn_3.component.id == x.component.id]
        assert len(connection) == 1

    def test_run_1_to_1(self):
        test = Test1ToN("Test 1 to 1", Test1ToNTest.pg_test)
        test.add_output(Test1ToNTest.proc_3)
        test.add_input(Test1ToNTest.proc_2)

        content_string = "This is the content of the test message"

        message = FlowFile(content_string, {"attribute1": "value1"})
        result = test.run("Processor 2", message)
        # Check result
        assert result.content == content_string
        assert result.attributes['test_input_name'] == self.proc_2.component.name
        assert result.attributes['test_output_name'] == self.proc_3.component.name
        assert int(result.attributes['test_start_time']) > 0
        assert int(result.attributes['test_end_time']) > 0
        assert int(result.attributes['test_duration']) == int(result.attributes['test_end_time']) - int(result.attributes['test_start_time']), "id is not an integer: %r" % id

        # Check is test was cleaned up nicely
        # The test process group should be removed
        assert len(canvas.list_all_process_groups(Test1ToNTest.pg_test.component.id)) == 1  # Only counting self
        # Check if connections were built again; there should be 3 now
        assert len(canvas.list_all_connections(Test1ToNTest.pg_test.component.id, descendants=False)) == 4

    # def test_run_1_to_n(self):
    #     test = Test1ToN("Test 1 to N", Test1ToNTest.pg_test)
    #     test.add_output(Test1ToNTest.proc_end_1)
    #     test.add_output(Test1ToNTest.proc_end_2)
    #     test.add_input(Test1ToNTest.proc_2)
    #
    #     content_string = "This is the content of the test message"
    #
    #     message = FlowFile(content_string, {"attribute1": "value1"})
    #     result = test.run("Processor 2", message)
    #     # Check result
    #     assert result.content == content_string
    #     assert result.attributes['test_input_name'] == self.proc_2.component.name
    #     assert result.attributes['test_output_name'] == self.proc_3.component.name
    #     assert int(result.attributes['test_start_time']) > 0
    #     assert int(result.attributes['test_end_time']) > 0
    #     assert int(result.attributes['test_duration']) == int(result.attributes['test_end_time']) - int(result.attributes['test_start_time']), "id is not an integer: %r" % id
    #
    #     # Check is test was cleaned up nicely
    #     # The test process group should be removed
    #     assert len(canvas.list_all_process_groups(Test1ToNTest.pg_test.component.id)) == 1  # Only counting self
    #     # Check if connections were built again; there should be 3 now
    #     assert len(canvas.list_all_connections(Test1ToNTest.pg_test.component.id, descendants=False)) == 4


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
