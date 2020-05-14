"""
Created on 18 Feb 2019

@author: Frank Ypma
"""


class FlowFile(object):
    """"
    Mock object for a nifi message, containing flowfile content and a dict of attributes
    For now, only string content is accepted, but that could be expanded
    """

    def __init__(self, content="", attributes=None):
        assert isinstance(content, str)
        assert isinstance(attributes, dict)
        
        self.content = content
        if attributes is None:
            self.attributes = {}
        else:
            self.attributes = attributes
            
    def __str__(self):
        return f"{{'content': '{self.content}', 'attributes': {self.attributes!r}}}"

    __repr__ = __str__
