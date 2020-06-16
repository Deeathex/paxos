**To run:**  
cd ConsensusAlgorithm\src\main\resources\paxos-reference-binaries  
paxos-windows-amd64.exe localhost 5000 localhost 5001 5002 5003  // to start the hub
paxos> list  // list the nodes available in hub
paxos> test ref node // start the consensus between ref and node nodes registered to the hub
