# NiPyTest

Unit test framework for nifi dataflows

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

To be able to test this test library, you will have to run a nifi instance. This can be virtual machine.

The connection url to the machine is configured in conf/nipytest.conf

### Installing

This library does not build with python 3.8, use 3.6/3.7.

Setup a venv (or use any other env), and install the requirements using
```
pip3 install -r reuqirements.txt
```

(Example venv creation)
```
virtualenv venv && source venv/bin/activate
```

## Running the tests

All tests are in the /tests folder. To run them all:

```
python -m unittest discover tests
```

## Deployment

TODO

## Contributing

This project was started as a way to learn python. It is not finished and it was not used in an enterprise environment.
It will not be continued. Feel free to fork or copy any part of the contents and develop it further.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Frank Ypma** - *Initial work* - [fhcypma](https://github.com/fhcypma)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
