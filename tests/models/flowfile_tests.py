"""
Created on 22 Feb 2019

@author: Frank Ypma
"""
import unittest
from nipytest.models.flowfile import FlowFile


class TestFlowFile(unittest.TestCase):

    def test_init(self):
        FlowFile("content", {"attribute1": "value1"})
        
    def test_to_string(self):
        msg = FlowFile("content '", {"attribute1": "value1"})
        assert msg.__str__() == "{'content': 'content \'', 'attributes': {'attribute1': 'value1'}}"


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
