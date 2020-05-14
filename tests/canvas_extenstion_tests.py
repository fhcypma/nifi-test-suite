"""
Created on 22 Feb 2019

@author: Frank Ypma
"""
import unittest
from nipyapi import nifi, config, canvas
from nipytest import canvas_extension as canvas_ext
from nipytest.canvas_navigator import CanvasNavigator
from nipytest.models.location import Location

PORT = 80


class CanvasExtensionTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        config.nifi_config.host = 'http://192.168.56.5:8080/nifi-api'

    def test_create_http_context_map(self):
        nav = CanvasNavigator()
        name = "nipytest - unit test - test_create_http_context_map"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_http_context_map("id", "name")
            # noinspection PyTypeChecker
            canvas_ext.create_http_context_map(nav.current, 1)
        # Run function
        controller = canvas_ext.create_http_context_map(nav.current, name) 
        # If output is ok type, function was successful
        self.assertIsInstance(controller, nifi.ControllerServiceEntity)
        self.assertIn(controller.component.state, ["ENABLED", "ENABLING"])
        # Remove temporary created object(s)
        canvas.delete_controller(controller, True)

    def test_create_request_handler(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_request_handler"
        controller = canvas_ext.create_http_context_map(nav.current, name) 
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_request_handler("id", loc, controller, PORT)
            # noinspection PyTypeChecker
            canvas_ext.create_request_handler(nav.current, 1, controller, PORT)
            canvas_ext.create_request_handler(nav.current, loc, 1, PORT)
            # noinspection PyTypeChecker
            canvas_ext.create_request_handler(nav.current, loc, controller, "string")
        # Run function
        request_handler = canvas_ext.create_request_handler(nav.current, loc, controller, PORT)
        # If output is ok type, function was successful
        self.assertIsInstance(request_handler, nifi.ProcessorEntity)
        self.assertEqual(request_handler.component.config.properties["Listening Port"], str(PORT))
        # Remove temporary created object(s)
        canvas.delete_processor(request_handler, force=True)
        canvas.delete_controller(controller, True)
        
    def test_create_input_router(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_input_router"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_input_router("id", loc, [])
            # noinspection PyTypeChecker
            canvas_ext.create_input_router(nav.current, 1, [])
#             canvas_ext.create_input_router(nav.current, loc, 1)
        # Run function
        output_port = canvas_ext.create_test_input(nav.current, loc, name)
        input_router = canvas_ext.create_input_router(nav.current, loc, [output_port])
        # If output is ok type, function was successful
        self.assertIsInstance(input_router, nifi.ProcessorEntity)
        self.assertIsNotNone(input_router.component.config.properties[name])
        # Remove temporary created object(s)
        canvas.delete_processor(input_router, force=True)
        canvas.delete_port(output_port)
        
    def test_create_test_input(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_test_input"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_test_input("id", loc, name)
            # noinspection PyTypeChecker
            canvas_ext.create_test_input(nav.current, 1, name)
            # noinspection PyTypeChecker
            canvas_ext.create_test_input(nav.current, loc, 1)
        # Run function
        output_port = canvas_ext.create_test_input(nav.current, loc, name)
        # If output is ok type, function was successful
        self.assertIsInstance(output_port, nifi.PortEntity)
        self.assertEqual(output_port.component.name, name)
        # Remove temporary created object(s)
        canvas.delete_port(output_port)       

    def test_create_output_router(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_output_router"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_output_router("id", loc, name)
            # noinspection PyTypeChecker
            canvas_ext.create_output_router(nav.current, 1, name)
            # noinspection PyTypeChecker
            canvas_ext.create_output_router(nav.current, loc, 1)
        # Run function
        output_router = canvas_ext.create_output_router(nav.current, loc, name)
        # If output is ok type, function was successful
        self.assertIsInstance(output_router, nifi.ProcessorEntity)
        self.assertIsNotNone(output_router.component.name, name)
        # Remove temporary created object(s)
        canvas.delete_processor(output_router, force=True)

    def test_create_response_handler(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_response_handler"
        controller = canvas_ext.create_http_context_map(nav.current, name) 
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_response_handler("id", loc, controller)
            # noinspection PyTypeChecker
            canvas_ext.create_response_handler(nav.current, 1, controller)
            canvas_ext.create_response_handler(nav.current, loc, 1)
        # Run function
        response_handler = canvas_ext.create_response_handler(nav.current, loc, controller)
        # If output is ok type, function was successful
        self.assertIsInstance(response_handler, nifi.ProcessorEntity)
        # Remove temporary created object(s)
        canvas.delete_processor(response_handler, force=True)
        canvas.delete_controller(controller, True)
        
    def test_create_test_output(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_test_output"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_test_output("id", loc, name)
            # noinspection PyTypeChecker
            canvas_ext.create_test_output(nav.current, 1, name)
            # noinspection PyTypeChecker
            canvas_ext.create_test_output(nav.current, loc, 1)
        # Run function
        input_port = canvas_ext.create_test_output(nav.current, loc, name)
        # If output is ok type, function was successful
        self.assertIsInstance(input_port, nifi.PortEntity)
        self.assertEqual(input_port.component.name, name)
        # Remove temporary created object(s)
        canvas.delete_port(input_port)
    
    def test_create_output_attribute(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_create_output_attribute"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.create_output_attribute("id", loc, name)
            # noinspection PyTypeChecker
            canvas_ext.create_output_attribute(nav.current, 1, name)
            # noinspection PyTypeChecker
            canvas_ext.create_output_attribute(nav.current, loc, 1)
        # Run function
        output_attribute = canvas_ext.create_output_attribute(nav.current, loc, name)
        # If output is ok type, function was successful
        self.assertIsInstance(output_attribute, nifi.ProcessorEntity)
        self.assertEqual(output_attribute.component.name, "Set output name '"+name+"'")
        # Remove temporary created object(s)
        canvas.delete_processor(output_attribute)
    
    def test_recreate_connection(self):
        nav = CanvasNavigator()
        loc = Location()
        name = "nipytest - unit test - test_recreate_connection"
        # Testing incorrect inputs
        with self.assertRaises(AssertionError):
            canvas_ext.recreate_connection(1)
        # Create connection
        input_port = canvas_ext.create_test_output(nav.current, loc, name)
        output_port = canvas_ext.create_test_input(nav.current, loc, name)
        connection = canvas.create_connection(input_port, output_port)
        canvas.delete_connection(connection)
        # Run function
        new_connection = canvas_ext.recreate_connection(connection)
        # If output is ok type, function was successful
        self.assertIsInstance(new_connection, nifi.ConnectionEntity)
        self.assertNotEqual(connection.component.id, new_connection.component.id)
        # Remove temporary created object(s)
        canvas.delete_connection(new_connection)
        canvas.delete_port(input_port)
        canvas.delete_port(output_port)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
