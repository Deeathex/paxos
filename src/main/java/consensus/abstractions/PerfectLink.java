package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.system.NetworkManager;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.PL_SEND;

/**
 * Sends any PL send messages to theis destination using the network manager.
 */
public class PerfectLink implements Abstraction {
    private final ConsensusSystem system;

    public PerfectLink(ConsensusSystem system) {
        this.system = system;
    }

    @Override
    public boolean handle(Message message) {
        if (PL_SEND.equals(message.getType())) {
            ProcessId destinationProcess = message.getPlSend().getDestination();
            NetworkManager.sendMessage(message, destinationProcess.getHost(), destinationProcess.getPort(), system.getNodePort());
            return true;
        }
        return false;
    }
}
