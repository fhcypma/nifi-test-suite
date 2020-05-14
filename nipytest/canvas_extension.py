"""
Created on 20 Feb 2019

@author: Frank Ypma

Extension to the nipyapi canvas module, used to create the objects on the
canvas to create the tests
"""
from nipyapi import nifi, canvas
from nipytest.models.location import Location


def create_http_context_map(parent_pg, name):
    """
    Creates and enables a StandardHttpContextMap
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            controller
        name (string): Name of the controller
        
    Returns:
        (ControllerServiceEntity)
    """
    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(name, str)

    http_context = nifi.ProcessGroupsApi().create_controller_service(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.http.StandardHttpContextMap",
                name=name
            )
        )
    )
    # canvas.create_controller not working properly...
    return nifi.ControllerServicesApi().update_controller_service(
        id=http_context.component.id,
        body=nifi.ControllerServiceEntity(
            revision=nifi.RevisionDTO(version=1),
            component=nifi.ControllerServiceDTO(
                state="ENABLED",
                id=http_context.component.id
            )
        )
    )


def create_request_handler(parent_pg, location, http_context, port):
    """
    Creates a HandleHttpRequest and connects it to the Http controller service
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        http_context (ControllerServiceEntity): StandardHttpContextMap to
            connect the processor to
        port (int): port number to use
        
    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(http_context, nifi.ControllerServiceEntity)
    assert isinstance(port, int)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.HandleHttpRequest",
                name="Receive test message",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        "HTTP Context Map": http_context.component.id,
                        "Listening Port": port
                    }
                )
            )
        )
    )


def create_input_attribute(parent_pg, location, name):
    """
    Creates a UpdateAttribute to register the test_start_time to the
        flowfile

    Args:
        parent_pg (ProcessGroupEntity): Target process group to place
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test output

    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
                name=name,
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        "test_start_time": "${now():toNumber()}"
                    }
                )
            )
        )
    )


def create_input_router(parent_pg, location, test_inputs):
    """
    Creates a RouteOnAttribute, routing each different input
    Only flow files with a matching http.headers.test_input_name attribute
        will be routed to an (corresponding) input
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        test_inputs (array of ProcessorEntity, InputPortEntity and OutputPortEntity):
            array of outputs for the router, routed to the different inputs
        
    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    #     assert isinstance(test_inputs, array)

    route_properties = {}
    for test_input in test_inputs:
        route_properties[test_input.component.name] = "${http.headers.test_input_name:equals('" \
                                                      + test_input.component.name + "')}"

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.RouteOnAttribute",
                name="Route to correct input",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties=route_properties,
                    auto_terminated_relationships=["unmatched"]
                )
            )
        )
    )


def create_test_input(parent_pg, location, name):
    """
    Creates a test input (as an output port in the test process
        group)
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test input
        
    Returns:
        (OutputPortEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_output_port(
        id=parent_pg.component.id,
        body=nifi.PortEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.PortDTO(
                name=name,
                position=nifi.PositionDTO(
                    x=location.x + 50,
                    y=location.y
                )
            )
        )
    )


def create_output_router(parent_pg, location, name):
    """
    Creates a RouteOnAttribute to filter all incoming messages from
        the actual test results 
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test input
        
    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.RouteOnAttribute",
                name="Filter only test messages",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={"test": "${http.request.uri:equals('/" + name + "')}"},
                    auto_terminated_relationships=["unmatched"]
                )
            )
        )
    )


def create_output_replacetext(parent_pg, location, name):
    """
    Creates a ReplaceText to write attributes into the flowfile. When
        we later merge the contents, we would otherwise lose the
        attributes

    Args:
        parent_pg (ProcessGroupEntity): Target process group to place
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test input

    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.ReplaceText",
                name="Merge flowfile and attributes",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        # "Search Value": "^(.*)$",
                        "Replacement Value": "{\"flowfile\":\"$1\",\"attributes\":{" +
                                             "\"test_start_time\":\"${test_start_time}\"," +
                                             "\"test_end_time\":\"${test_end_time}\"," +
                                             "\"test_duration\":\"${test_duration}\"," +
                                             "\"test_input_name\":\"${http.headers.test_input_name}\"," +
                                             "\"test_output_name\":\"${test_output_name}\"" +
                                             "}}"
                    },
                    auto_terminated_relationships=["failure"]
                )
            )
        )
    )

def create_output_mergecontent(parent_pg, location, name):
    """
    Creates a MergeContent to combine flowfiles from different outputs

    Args:
        parent_pg (ProcessGroupEntity): Target process group to place
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test input

    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.MergeContent",
                name="Merge outputs",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        "Header": "[",
                        "Footer": "]",
                        "Demarcator": ","
                    },
                    auto_terminated_relationships=["failure", "original"]
                )
            )
        )
    )

def create_response_handler(parent_pg, location, http_context):
    """
    Creates a HandleHttpResponse and connects it to the Http controller service
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        http_context (ControllerServiceEntity): StandardHttpContextMap to
            connect the processor to
        
    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(http_context, nifi.ControllerServiceEntity)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.standard.HandleHttpResponse",
                name="Return test result",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        "HTTP Context Map": http_context.component.id,
                        "HTTP Status Code": "200",
                        "test_input_name": "${http.headers.test_input_name}",
                        "test_output_name": "${test_output_name}",
                        "test_start_time": "${test_start_time}",
                        "test_end_time": "${test_end_time}",
                        "test_duration": "${test_duration}"
                    },
                    auto_terminated_relationships=["failure", "success"]
                )
            )
        )
    )


def create_test_output(parent_pg, location, name):
    """
    Creates a test output (as an input port in the test process
        group)
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test output
        
    Returns:
        (InputPortEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_input_port(
        id=parent_pg.component.id,
        body=nifi.PortEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.PortDTO(
                name=name,
                position=nifi.PositionDTO(
                    x=location.x + 50,
                    y=location.y
                )
            )
        )
    )


def create_output_attribute(parent_pg, location, name):
    """
    Creates a UpdateAttribute to register the test_output_name to the
        response
    
    Args:
        parent_pg (ProcessGroupEntity): Target process group to place 
            processor
        location (Location): x,y coordinated to place the processor
        name (string): Name of the test output
        
    Returns:
        (ProcessorEntity)
    """

    assert isinstance(parent_pg, nifi.ProcessGroupEntity)
    assert isinstance(location, Location)
    assert isinstance(name, str)

    return nifi.ProcessGroupsApi().create_processor(
        id=parent_pg.component.id,
        body=nifi.ProcessorEntity(
            revision=nifi.RevisionDTO(version=0),
            component=nifi.ProcessorDTO(
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
                name="Set output name '" + name + "'",
                position=nifi.PositionDTO(
                    x=location.x,
                    y=location.y
                ),
                config=nifi.ProcessorConfigDTO(
                    properties={
                        "test_output_name": name,
                        "test_end_time": "${now():toNumber()}",
                        "test_duration": "${now():toNumber():minus(${test_start_time})}"
                    }
                )
            )
        )
    )


def recreate_connection(connection):
    """
    Creates the connection provided (assuming it's been deleted 
        from the canvas before)
    
    Args:
        connection (ConnectionEntity): Connection to re-create
    
    Returns:
        (ConnectionEntity)
    """
    assert isinstance(connection, nifi.ConnectionEntity)

    # Removing connection id; cannot specify it on re-creation
    connection.component.id = None
    # Re-create connection
    return nifi.ProcessGroupsApi().create_connection(
        id=connection.component.parent_group_id,
        body=nifi.ConnectionEntity(
            revision=nifi.RevisionDTO(version=0),
            source_type=connection.source_type,
            destination_type=connection.destination_type,
            component=connection.component
        )
    )


def delete_all_connections(process_group, purge=True, descendants=True):
    """
    Purges and deletes all connections inside a process group

    Args:
        process_group (ProcessGroupEntity): Process group where connections should be purged and deleted
        purge (bool): True to Purge, Defaults to True
        descendants (bool): True to recurse child PGs, False to not
    """
    assert isinstance(process_group, nifi.ProcessGroupEntity)

    connections = canvas.list_all_connections(process_group.component.id, descendants=descendants)
    print("Listing connections in " + process_group.component.id)
    print(connections)
    for connection in connections:
        canvas.delete_connection(connection, purge=purge)
