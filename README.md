## A Distributed Resource AssiGnment and Orchestration algorithm with Guarantees

Updated Jul 30, 2018


#### @Copyright
DRAGON is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
DRAGON is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with DRAGON. If not, see <http://www.gnu.org/licenses/>.


#### About the project

This repository provides the prototype of an architecture that cenables coexistence of different applications over the same shared edge infrastructure. To dynamically partition resources to applications, this project uses the Distributed Resource AssiGnment OrchestratioN (DRAGON), an approximation algorithm that we designed to seek optimal resource partitioning between applications and guarantees both a bound on convergence time and an optimal (1-1/e)-approximation with respect to the Pareto optimal resource assignment.


#### Repository Structure

The repository tree contains: 


* [README.md]()  
    --this file  
* [LICENSE]()  
    --GPLv3 license  
* [main.py]()  
    --main instance executable  
* [config/]()  
    --configuration files  
* [dragon\_agent/]()  
    --DRAGON agent source files  
* [resource\_assignment/]()  
    --implementation of the applications-resources assignment problem  
* [scripts/]()  
    --useful scripts related to the project  
* [tests/]()  
    --validation purpose scripts  
* [use\_cases\_simulations/]()  
    --simulation environment that runs two edge use case over DRAGON


### Configuration

[config/config.py]() -- agent and instance configuration  
[config/rap\_instance.json]() -- resource assignment problem instance values


### Install

This project requires python 3.6 and has been tested on Linux debian (testing) with kernel 4.16.0-2-amd64.

Some additional python packages are required:

    $ sudo apt install python3-pip
    $ sudo pip3 install pika==0.12.0 colorlog==3.1.4
    
Inter agent communication is implemented over the RabbitMQ Broker. To install it use the following command: 

    $ sudo apt install rabbitmq-server
    

### Run

Make sure rabbitmq is running:

    $ sudo service rabbitmq-server start

The [main.py]() script runs a single instance of the DRAGON agent. To run it, use the following command from the project root directory:

    $ python3 main.py {agent-name} {services} [-d {configuration-file}]
    
where:

- ***agent-name***: is a name to identify the agent;
- ***services***: is a list of parameters, namely the names of services for which the agent will attempt to obtain resources (see [config/rap\_instance.json]()).
- ***configuration-file***: is the path of the configuration file to use (default is [config/default-config.ini]()).


#### Testing

The [tests/]() folder also contains a script that automatically runs multiple agents at the same time. 
Please modify [config/default-config.ini]() as desired before to run it, so to specify instance parameters, then use:

    $ python3 -m tests.test_script
    
The number of agent specified in the configuration file (each with a random number of services) will be run and the script will wait for convergence.
At the end of the execution, the log file of each agent will be available in the main folder, while details on the resulting assignments will be stored on the (generated) [results]() folder.
