package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.utils.ValueUtils;

import java.util.HashMap;
import java.util.Map;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.*;

/**
 * Epoch consensus is a primitive similar to consensus, where the processes propose a value
 * and may decide a value. Every epoch is identified by an epoch timestamp and has a designated leader.
 * The goal of epoch consensus is that all processes, regardless whether correct or faulty, decide the
 * same value, and it only represents an attempt to reach consensus; epoch consensus may not terminate
 * and can be aborted when it does not decide or when the next epoch should already be started by the
 * higher-level algorithm.
 * <p>
 * The bellow implementation is a Read/Write epoch consensus. Every process runs at most one epoch
 * consensus at a time and different instances never interfere with each other. The leader tries to
 * impose a decision value on the processes. The other processes witness the actions of the leader,
 * should the leader fail, and they also witness actions of leaders in earlier epochs.
 * <p>
 * The algorithm involves two rounds of message exchanges from the leader to all processes.
 * The goal is for the leader to write its proposal value to all processes, who store the epoch
 * timestamp and the value in their state and acknowledge this to the leader. When the leader
 * receives enough acknowledgments, it will ep-decide this value.
 * <p>
 * When aborted, the epoch consensus implementation simply returns its state, consisting of the
 * timestamp/value pair with the written value, and halts. It is important that the instance performs
 * sno further steps.
 */
public class EpochConsensus implements Abstraction {
    private final ConsensusSystem system;
    private final int ets;
    private EpState_ state;
    private Value tmpVal;
    private Map<Integer, EpState_> states = new HashMap<>();
    private int accepted;
    private boolean halted;

    public EpochConsensus(ConsensusSystem system, int ets, EpState_ state) {
        this.system = system;

        this.ets = ets;
        this.state = state;
        this.tmpVal = ValueUtils.getUndefinedValue();
        this.accepted = 0;
        this.halted = false;
    }

    @Override
    public boolean handle(Message message) {
        if (halted) {
            return false;
        }
        switch (message.getType()) {
            case EP_PROPOSE:
                handleEpPropose(message.getEpPropose().getValue());
                return true;
            case BEB_DELIVER:
                BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case EP_READ_:
                        handleBebDeliverEpRead(bebDeliver.getSender());
                        return true;
                    case EP_WRITE_:
                        handleBebDeliverEpWrite(bebDeliver);
                        return true;
                    case EP_DECIDED_:
                        handleBebDeliverEpDecided(bebDeliver.getMessage().getEpDecided().getValue());
                        return true;
                }
                return false;
            case PL_DELIVER:
                PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case EP_STATE_:
                        handlePlDeliverEpState(plDeliver);
                        return true;
                    case EP_ACCEPT_:
                        handlePlDeliverEpAccept();
                        return true;
                }
                return false;
            case EP_ABORT:
                handleEpAbort();
                return true;
        }
        return false;
    }

    /**
     * At first, the leader reads the state of the processes by sending a READ message.
     *
     * @param epProposeValue
     */
    private void handleEpPropose(Value epProposeValue) {
        tmpVal = epProposeValue;
        system.trigger(Message.newBuilder()
                .setType(BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(EP_READ_)
                                .setEpRead(EpRead_.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build());
    }

    /**
     * Every process answers with a STATE message containing its locally stored value and the
     * timestamp of the epoch during which the value was last written.
     *
     * @param sender the process that answers with a STATE message
     */
    private void handleBebDeliverEpRead(ProcessId sender) {
        system.trigger(Message.newBuilder()
                .setType(PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(sender)
                        .setMessage(Message.newBuilder()
                                .setType(EP_STATE_)
                                .setEpState(EpState_.newBuilder()
                                        .setValueTimestamp(state.getValueTimestamp())
                                        .setValue(state.getValue())
                                        .build())
                                .build())
                        .build())
                .build());
    }

    /**
     * All processes receive from leader l the chosen value and accept the message
     * and store the value locally.
     *
     * @param bebDeliver the message
     */
    private void handleBebDeliverEpWrite(BebDeliver bebDeliver) {
        state = EpState_.newBuilder()
                .setValueTimestamp(ets)
                .setValue(bebDeliver.getMessage().getEpWrite().getValue())
                .build();
        system.trigger(Message.newBuilder()
                .setType(PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(bebDeliver.getSender())
                        .setMessage(Message.newBuilder()
                                .setType(EP_ACCEPT_)
                                .setEpAccept(EpAccept_.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build());
    }

    /**
     * Receive a decided message and decide upon that value.
     *
     * @param epDecidedValue the decided value
     */
    private void handleBebDeliverEpDecided(Value epDecidedValue) {
        system.trigger(Message.newBuilder()
                .setType(EP_DECIDE)
                .setEpDecide(EpDecide.newBuilder()
                        .setEts(ets)
                        .setValue(epDecidedValue)
                        .build())
                .build());
    }

    /**
     * The leader receives a quorum of STATE messages and choses the value that comes with the highest
     * timestamp as its proposal value, if one exists. The leader then writes the chosen value to all
     * processes with a WRITE message.
     *
     * @param plDeliver the message
     */
    private void handlePlDeliverEpState(PlDeliver plDeliver) {
        EpState_ epState = plDeliver.getMessage().getEpState();
        states.put(plDeliver.getSender().getPort(), EpState_.newBuilder()
                .setValueTimestamp(epState.getValueTimestamp())
                .setValue(epState.getValue())
                .build());
        if (states.size() > system.getProcessList().size() / 2) {
            Value maxValue = ValueUtils.getUndefinedValue();
            int maxTimestamp = -1;
            for (EpState_ state : states.values()) {
                if (state.getValueTimestamp() > maxTimestamp) {
                    maxValue = state.getValue();
                    maxTimestamp = state.getValueTimestamp();
                }
            }
            if (maxValue.getDefined()) {
                tmpVal = maxValue;
            }
            states.clear();
            system.trigger(Message.newBuilder()
                    .setType(BEB_BROADCAST)
                    .setBebBroadcast(BebBroadcast.newBuilder()
                            .setMessage(Message.newBuilder()
                                    .setType(EP_WRITE_)
                                    .setEpWrite(EpWrite_.newBuilder()
                                            .setValue(ValueUtils.makeCopy(tmpVal))
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }

    /**
     * The write succeeds when the leader receives an ACCEPT message from a quorum of processes,
     * indicating that they have stored the value locally. The leader now ep-decides the chosen
     * value and announces this in a DECIDED message to all processes; the processes that receive
     * this ep-decide as well.
     */
    private void handlePlDeliverEpAccept() {
        accepted++;
        if (accepted > system.getProcessList().size() / 2) {
            accepted = 0;
            system.trigger(Message.newBuilder()
                    .setType(BEB_BROADCAST)
                    .setBebBroadcast(BebBroadcast.newBuilder()
                            .setMessage(Message.newBuilder()
                                    .setType(EP_DECIDED_)
                                    .setEpDecided(EpDecided_.newBuilder()
                                            .setValue(ValueUtils.makeCopy(tmpVal))
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }

    /**
     * When aborted, the epoch consensus implementation simply returns its state, consisting
     * of the timestamp/value pair with the written value, and halts.
     */
    private void handleEpAbort() {
        system.trigger(Message.newBuilder()
                .setType(EP_ABORTED)
                .setEpAborted(EpAborted.newBuilder()
                        .setEts(ets)
                        .setValueTimestamp(state.getValueTimestamp())
                        .setValue(ValueUtils.makeCopy(state.getValue()))
                        .build())
                .build());
        halted = true;
    }
}
