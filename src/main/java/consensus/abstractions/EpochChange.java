package consensus.abstractions;

import consensus.system.ConsensusSystem;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.*;

/**
 * An epoch change abstraction (leader based) that signals the start of a new epoch when
 * a leader is suspected. Every process maintains a timestamp lastTs (last epoch that it started)
 * and a timestamp ts (last epoch that it attempted to start with itself as a leader).
 * Initially, the process sets ts to its rank. Whenever the leader detector subsequently makes p
 * trust itself, p adds N to ts and sends a NEWEPOCH message with ts.
 */
public class EpochChange implements Abstraction {
    private final String EC = "ec";
    private ConsensusSystem system;

    private int lastTs;
    private int ts;
    private ProcessId trusted;

    public EpochChange(ConsensusSystem system) {
        this.system = system;

        this.lastTs = 0;
        this.ts = system.getCurrentProcess().getRank();
        this.trusted = system.getMinRankProcess();
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case ELD_TRUST:
                handleEldTrust(message.getEldTrust().getProcess());
                return true;
            case BEB_DELIVER:
                BebDeliver bebDeliver = message.getBebDeliver();
                if (EC_NEW_EPOCH_.equals(bebDeliver.getMessage().getType())) {
                    handleBebDeliverEcNewEpoch(bebDeliver);
                    return true;
                }
                return false;
            case PL_DELIVER:
                PlDeliver plDeliver = message.getPlDeliver();
                if (EC_NACK_.equals(plDeliver.getMessage().getType())) {
                    handlePlDeliver();
                    return true;
                }
                return false;
        }
        return false;
    }

    /**
     * Whenever the leader detector subsequently makes p trust itself, p adds N to ts and sends a
     * NEWEPOCH message with ts.
     *
     * @param eldTrustProcess the process to be trusted
     */
    private void handleEldTrust(ProcessId eldTrustProcess) {
        trusted = eldTrustProcess;
        if (trusted.getRank() == system.getCurrentProcess().getRank()) {
            ts += system.getProcessList().size();
            system.trigger(Message.newBuilder()
                    .setType(BEB_BROADCAST)
                    .setBebBroadcast(BebBroadcast.newBuilder()
                            .setMessage(Message.newBuilder()
                                    .setAbstractionId(EC)
                                    .setType(EC_NEW_EPOCH_)
                                    .setEcNewEpoch(EcNewEpoch_.newBuilder()
                                            .setTimestamp(ts)
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }

    /**
     * When process p receives a NEWEPOCH message with a parameter
     * newts > lastts from some process l and p most recently trusted l, then the
     * process triggers a StartEpoch event with parameters newts and l. Otherwise, the
     * process informs the aspiring leader l with a NACK message that the new epoch could
     * not be started.
     *
     * @param bebDeliver the message
     */
    private void handleBebDeliverEcNewEpoch(BebDeliver bebDeliver) {
        int newTs = bebDeliver.getMessage().getEcNewEpoch().getTimestamp();
        if (bebDeliver.getSender().getPort() == trusted.getPort() && newTs > lastTs) {
            lastTs = newTs;
            system.trigger(Message.newBuilder()
                    .setAbstractionId(EC)
                    .setType(EC_START_EPOCH)
                    .setEcStartEpoch(EcStartEpoch.newBuilder()
                            .setNewTimestamp(newTs)
                            .setNewLeader(bebDeliver.getSender())
                            .build())
                    .build());
        } else {
            system.trigger(Message.newBuilder()
                    .setType(PL_SEND)
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(bebDeliver.getSender())
                            .setMessage(Message.newBuilder()
                                    .setAbstractionId(EC)
                                    .setType(EC_NACK_)
                                    .setEcNack(EcNack_.newBuilder()
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }

    /**
     * When a process receives a NACK message and still trusts itself, it increments
     * ts by N and tries again to start an epoch by sending another NEWEPOCH
     * message.
     */
    private void handlePlDeliver() {
        if (trusted.getPort() == system.getCurrentProcess().getPort()) {
            ts += system.getProcessList().size();
            system.trigger(Message.newBuilder()
                    .setType(BEB_BROADCAST)
                    .setBebBroadcast(BebBroadcast.newBuilder()
                            .setMessage(Message.newBuilder()
                                    .setAbstractionId(EC)
                                    .setType(EC_NEW_EPOCH_)
                                    .setEcNewEpoch(EcNewEpoch_.newBuilder()
                                            .setTimestamp(ts)
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }
}
