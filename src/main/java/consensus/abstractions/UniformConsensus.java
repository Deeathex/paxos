package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.utils.ValueUtils;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.*;

/**
 * A uniform consensus algorithm based on a fail-noisy model (Leader driven consensus) that runs
 * through a sequence of epochs. The value that is decided by the consensus algorithm is the value
 * that is ep-decided by one of the underlying epoch consensus instances. To switch from one epoch
 * to the next, the algorithm aborts the running epoch consensus, obtains its state, and initializes
 * the next epoch consensus with the state. Hence, the algorithm invokes a well-formed sequence of
 * epoch consensus instances.
 * <p>
 * As soon as a process has obtained the proposal value for consensus from the
 * application and the process is also the leader of the current epoch, it ep-proposes this
 * value for epoch consensus. When the current epoch ep-decides a value, the process
 * also decides that value in consensus, but continues to participate in the consensus
 * algorithm, to help other processes decide.
 */
public class UniformConsensus implements Abstraction {
    private ConsensusSystem system;

    private Value val;
    private boolean proposed;
    private boolean decided;
    private int ets;
    private ProcessId l;
    private int newts;
    private ProcessId newl;

    public UniformConsensus(ConsensusSystem system) {
        this.system = system;

        this.val = ValueUtils.getUndefinedValue();
        this.proposed = false;
        this.decided = false;

        this.ets = 0;
        this.l = system.getMinRankProcess();
        this.newts = 0;
        this.newl = null;

        system.startNewEpoch(this.ets, this.newts, this.val);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case UC_PROPOSE:
                handleUcPropose(message.getUcPropose().getValue());
                return true;
            case EC_START_EPOCH:
                handleEcStartEpoch(message.getEcStartEpoch());
                return true;
            case EP_ABORTED:
                EpAborted epAborted = message.getEpAborted();
                if (ets != epAborted.getEts()) {
                    return false;
                }
                handleEpAborted(epAborted);
                return true;
            case EP_DECIDE:
                EpDecide epDecide = message.getEpDecide();
                if (ets != epDecide.getEts()) {
                    return false;
                }
                handleEpDecide(epDecide.getValue());
                return true;
        }
        return false;
    }

    /**
     * As soon as a process has obtained the proposal value for consensus from the application and
     * the process is also the leader of the current epoch, it ep-proposes this value for epoch
     * consensus.
     *
     * @param ucProposeValue the proposal value for consensus
     */
    private void handleUcPropose(Value ucProposeValue) {
        val = ucProposeValue;
        checkToTrigger();
    }

    /**
     * A new epoch is started and the new timestamp and the new leader must
     * be established.
     *
     * @param ecStartEpoch
     */
    private void handleEcStartEpoch(EcStartEpoch ecStartEpoch) {
        newts = ecStartEpoch.getNewTimestamp();
        newl = ecStartEpoch.getNewLeader();
        system.trigger(Message.newBuilder()
                .setType(EP_ABORT)
                .setEpAbort(EpAbort.newBuilder()
                        .build())
                .build());
    }

    /**
     * The current epoch was successfully halted and a new epoch must be started.
     *
     * @param epAborted
     */
    private void handleEpAborted(EpAborted epAborted) {
        ets = newts;
        l = newl;
        proposed = false;
        system.startNewEpoch(ets, epAborted.getValueTimestamp(), epAborted.getValue());
        checkToTrigger();
    }

    /**
     * When a decision was made in the current epoch, that means that processes have arrived
     * to a consensus and the decided value is sent to the application layer.
     *
     * @param epDecideValue the decided value in the consensus algorithm
     */
    private void handleEpDecide(Value epDecideValue) {
        if (!decided) {
            decided = true;
            system.trigger(Message.newBuilder()
                    .setType(UC_DECIDE)
                    .setUcDecide(UcDecide.newBuilder()
                            .setValue(ValueUtils.makeCopy(epDecideValue))
                            .build())
                    .build());
        }
    }

    private void checkToTrigger() {
        if (l.getPort() == system.getCurrentProcess().getPort() && val.getDefined() && !proposed) {
            proposed = true;
            system.trigger(Message.newBuilder()
                    .setType(EP_PROPOSE)
                    .setEpPropose(EpPropose.newBuilder()
                            .setValue(ValueUtils.makeCopy(val))
                            .build())
                    .build());
        }
    }
}
