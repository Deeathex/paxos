package consensus.abstractions;

import consensus.system.ConsensusSystem;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.BEB_DELIVER;
import static consensus.Paxos.Message.Type.PL_SEND;

/**
 * A broadcast abstraction that enables a process to send a message, in a one-shot operation,
 * to all processes in a system, including itself.
 * Messages are unique, that is, no process ever broadcasts the same message
 * twice and furthermore, no two processes ever broadcast the same message.
 */
public class BestEffortBroadcast implements Abstraction {
    private static final String BEB = "beb";

    private ConsensusSystem system;

    public BestEffortBroadcast(ConsensusSystem system) {
        this.system = system;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                handleBebBroadcast(message.getBebBroadcast());
                return true;
            case PL_DELIVER:
                handlePlDeliver(message.getPlDeliver());
                return true;
        }
        return false;
    }

    /**
     * Broadcast the message given as parameter to everyone, including the current process (me).
     * @param bebBroadcast the message
     */
    private void handleBebBroadcast(BebBroadcast bebBroadcast) {
        for (ProcessId processId : system.getProcessList()) {
            system.trigger(Message.newBuilder()
                    .setType(PL_SEND)
                    .setAbstractionId(BEB)
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(processId)
                            .setMessage(bebBroadcast.getMessage())
                            .build())
                    .build());
        }
    }

    private void handlePlDeliver(PlDeliver plDeliver) {
        system.trigger(Message.newBuilder()
                .setType(BEB_DELIVER)
                .setAbstractionId(BEB)
                .setBebDeliver(BebDeliver.newBuilder()
                        .setSender(plDeliver.getSender())
                        .setMessage(plDeliver.getMessage())
                        .build())
                .build());
    }
}
