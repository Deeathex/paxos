**About the project**  
Paxos is a family of protocols for solving consensus in a network of unreliable processors 
(that is, processors that may fail). Consensus is the process of agreeing on one result among 
a group of participants. This problem becomes difficult when the participants or their communication 
medium may experience failures.

The project contains a binary that has the role of a hub that starts along with 3 nodes (this hub was 
made by Conf. dr. Boian Rares along the AMCDS (Algorithms, Models, and Concepts in Distributed Systems) 
course). The command to start the hub is given in the "To run" section. After starting the hub that manages 
all the nodes, the nodes implemented in this project should be started by running the main class. Afterwards,
the help command should be used in the paxos hub console to see the available commands. The command list
will list all the nodes registered to the hub. With this command, we can see after starting the main that
the 3 nodes of the application has successfully registered to the hub. After this, the consensus algorithm
should be started in order to trigger the decision upon the same value for all the nodes registered to the hub.
This is done by running the commands test ref node (test the implicit 3 nodes available in the hub with the
nodes developed in the application). 


**To run:**  
``` bash
cd ConsensusAlgorithm\src\main\resources\paxos-reference-binaries

paxos-windows-amd64.exe localhost 5000 localhost 5001 5002 5003 
```

The command from above will start the hub on localhost and will start the ref nodes directly available in the hub
on localhsot ports: 5001, 5002, 5003. After running this command, the project should be started by running the main.

Afterwards, in the paxos console (started from above) the user can trigger the following commands:

``` bash
paxos> list
```

The list command from above will show all the nodes registered to the hub.
(Which should be ref1, ref2, ref3 which are implicit in the hub and node1, node2, node3 which are
the nodes of the developed application) 

``` bash
paxos> test ref node
```

This command will start the consensus between ref and node nodes registered to the hub and all the
nodes should decide upon the same value.