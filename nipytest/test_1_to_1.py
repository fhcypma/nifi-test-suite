"""
Created on 10 Feb 2019

@author: Frank Ypma
"""

import logging
import requests

from nipyapi import nifi, canvas, config
from nipytest.canvas_navigator import CanvasNavigator
from nipytest.models.location import Location
from urllib.parse import urlparse
from nipytest.models.flowfile import FlowFile
from nipytest import canvas_extension as canvas_ext


class Test1To1(object):
    """
    Class for performing a test where a single input flowfile 
    leads to a single output flowfile (hence 1 to 1)
    """

    def __init__(self, name, base, port=80):
        """
        Prepares a test case with 1 incoming flowfile, expecting 1 returning flowfile. The test will be created on
            the canvas in the process group base with a name name.
            It is expected that config.nifi_config.host has already been set. E.g., 'http://<host>:8080/nifi-api'

        Args:
            name (str): The name of this test case
            base (nifi.ProcessGroupEntity): The process group to place the test; usually the process group where the
                flow resides
            port (int): The communication port
        """

        assert isinstance(name, str)
        assert isinstance(base, nifi.ProcessGroupEntity)
        assert isinstance(port, int)

        self.name = str.replace(name, " ", "_")
        self.base = base
        self.port = port
        self.__clear()
        
        self.logger = logging.getLogger('Test1To1')
        self.logger.setLevel(logging.DEBUG)

    def __clear(self):
        self.inputs = []
        self.outputs = []
        self.connections_to_remove = []
        self.test_group = None
        self.http_context = None
        self.http_in = None
        self.http_out = None

    def add_input(self, obj, remove_existing_connections=True):
        assert isinstance(obj, nifi.ProcessorEntity) or isinstance(obj, nifi.PortEntity)

        self.inputs.append(obj)
        if remove_existing_connections:
            # connections = canvas.list_all_connections(self.base.component.id, True)
            connections = canvas.list_all_connections()
            this_connections = [x for x in connections if obj.component.id == x.destination_id]
            self.connections_to_remove += this_connections

    def add_output(self, obj, remove_existing_connections=True):
        assert isinstance(obj, nifi.ProcessorEntity) or isinstance(obj, nifi.PortEntity)

        self.outputs.append(obj)
        if remove_existing_connections:
            # connections = canvas.list_all_connections(self.base.component.id, True)
            connections = canvas.list_all_connections()
            this_connections = [x for x in connections if obj.component.id == x.source_id]
            self.connections_to_remove += this_connections

    def __remove_outgoing_connections(self):
        for connection in self.connections_to_remove:
            canvas.delete_connection(connection, purge=True)
           
    def __restore_connections(self):
        for connection in self.connections_to_remove:
            canvas_ext.recreate_connection(connection)
        
    def run(self, input_name, flowfile, output_attributes=None, timeout=5):
        """
        Runs the actual test with the flowfile provided.
            Builds the test components on the nifi canvas
            Starts the base process group
            Post flowfile via http to initiate test
            Destroys all test components on the nifi canvas
            Returns output: flowfile in output.text, attributes in output.headers
        
        Args:
            input_name (str): The input to post the message to
            flowfile (FlowFile): The flowfile and attributes to post
            output_attributes (collections.Iterable of str): List of attributes to capture in the
                test output
            timeout (integer): Timeout in seconds. Will throw requests.exceptions.ReadTimeout
                when timeout expires
            
        Returns:
            (FlowFile)
        """
        assert isinstance(flowfile, FlowFile)
        
        # Set up testing infrastructure. This will stop the base
        self.__build()
        
        # Adding requested attributes as header parameters
        if output_attributes is not None:
            for attr in output_attributes:
                canvas.update_processor(self.http_out, nifi.ProcessorConfigDTO(properties={attr: "${"+attr+"}"}))
        
        # Start complete process group
        self.__start_base()
        
        # Prepare request
        parsed_url = urlparse(config.nifi_config.host)
        url = parsed_url.scheme+'://'+parsed_url.hostname+':'+str(self.port)+'/'+self.name
        headers = flowfile.attributes
        headers["test_input_name"] = input_name
        
        # Perform actual request
        response = requests.post(url, data=flowfile.content, headers=headers, timeout=timeout)

        # Clean up testing infrastructure
        self.__destroy()
        
        # Should always be 200
        assert response.status_code == 200
        
        response.headers.pop('Date')
        response.headers.pop('Transfer-Encoding')
        response.headers.pop('Server')
        return FlowFile(response.text, dict(response.headers))
    
    def __build(self):
        self.__stop_base()
        self.__create_test_group()
        self.__remove_outgoing_connections()
    
    def __destroy(self):
        self.__stop_base()
        self.__delete_test_group()
        self.__restore_connections()
        self.__clear()
        self.__start_base()
        
    def __delete_test_group(self):
        nav = CanvasNavigator()
        nav.cd_to_id(self.base.component.id)
        pgs = nav.groups(self.name)
        for pg in pgs:
            canvas.delete_process_group(pg, True)
        self.test_group = None
        
    def __create_test_group(self):
        # Delete group with same name if exists
        self.__delete_test_group()
        # Create group
        self.test_group = canvas.create_process_group(self.base, self.name, (0, 0))
        # Create contents
        self.__build_inputs()
        self.__build_outputs()
        
    def __build_inputs(self):
        # Create http context map for communication
        self.logger.debug("Creating 'StandardHttpContextMap' for communication")
        self.http_context = canvas_ext.create_http_context_map(self.test_group, self.name)
        
        # Keep track of "cursor" location on canvas
        location = Location()
        
        # Create http request for starting a test
        self.logger.debug("Creating 'HandleHttpRequest' for starting a test")
        self.http_in = canvas_ext.create_request_handler(self.test_group, location, self.http_context, self.port)

        location.y += 200

        # Set start time
        self.logger.debug("Creating 'UpdateAttribute' to set test start time")
        in_attribute = canvas_ext.create_input_attribute(self.test_group, location, "Set test start time")

        location.y += 200

        self.logger.debug("Connecting 'HandleHttpRequest' with 'UpdateAttribute'")
        canvas.create_connection(self.http_in, in_attribute, ["success"])

        # Route request to correct port
        self.logger.debug("Creating 'RouteOnAttribute' to send each request to the correct input port")
        in_route = canvas_ext.create_input_router(self.test_group, location, self.inputs)
        
        location.y += 200
        
        self.logger.debug("Connecting 'UpdateAttribute' with 'RouteOnAttribute'")
        canvas.create_connection(in_attribute, in_route, ["success"])
        
        location.y += 100  # Taking some extra vertical space, because we can have so many ports
        
        for test_input in self.inputs:
            input_name = test_input.component.name
            self.logger.debug("Creating port for input '%s'", input_name)
            output_port = canvas_ext.create_test_input(self.test_group, location, input_name)
            location.x += 400
            self.logger.debug("Connecting 'RouteOnAttribute' to port '%s'", input_name)
            canvas.create_connection(in_route, output_port, [input_name])
            self.logger.debug("Connecting port '%s' to processor '%s'", input_name, input_name)
            canvas.create_connection(output_port, test_input)
    
    def __build_outputs(self):
        # First creating response, so we can connect all outputs immediately in the loop
        location = Location(0, 1200)
        
        self.logger.debug("Creating RouteOnAttribute for filtering only test results")
        out_route = canvas_ext.create_output_router(self.test_group, location, self.name)
        
        location.y += 200
        
        self.logger.debug("Creating HandleHttpResponse for test results")
        self.http_out = canvas_ext.create_response_handler(self.test_group, location, self.http_context)
        canvas.create_connection(out_route, self.http_out, ["test"])
        
        location.x = 0
        for output in self.outputs:
            location.y = 800
            output_name = output.component.name
            self.logger.debug("Creating input_port for output '%s'", output_name)
            input_port = canvas_ext.create_test_output(self.test_group, location, output_name)
            location.y += 200
            self.logger.debug("Connecting processor '%s' to input_port '%s'", output_name, output_name)
            canvas.create_connection(output, input_port)
            
            self.logger.debug("Creating UpdateAttribute for output '%s'", output_name)
            update = canvas_ext.create_output_attribute(self.test_group, location, output_name)
            canvas.create_connection(input_port, update)
            canvas.create_connection(update, out_route)
            location.x += 400
        
    def __start_base(self):
        canvas.schedule_process_group(self.base.component.id, True)
    
    def __stop_base(self):
        canvas.schedule_process_group(self.base.component.id, False)
