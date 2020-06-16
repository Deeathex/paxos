package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.utils.ProcessListUtils;

import java.util.ArrayList;
import java.util.List;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.ELD_TRUST;

/**
 * An eventual leader-detector abstraction, that encapsulates a leader-election primitive
 * which ensures that eventually the correct processes will elect the same correct process
 * as their leader. Nothing precludes the possibility for leaders to change in an arbitrary
 * manner and for an arbitrary period of time. Moreover, many leaders might be elected during
 * the same period of time without having crashed. Once a unique leader is determined, and does
 * not change again, we say that the leader has stabilized.
 *
 * The Monarchical Eventual Leader Detection is implemented as a leader-detector abstraction.
 * The algorithm maintains the set of processes that are suspected and declares the nonsuspected
 * process with the highest rank to be the leader. Eventually, and provided at least one
 * process is correct, the same correct process will be trusted by all correct processes.
 */
public class EventualLeaderDetector implements Abstraction {
    private ConsensusSystem system;
    private final List<ProcessId> suspected = new ArrayList<>();
    private ProcessId leader = null;

    public EventualLeaderDetector(ConsensusSystem system) {
        this.system = system;
        updateTrustedLeader();
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                handleEpfdSuspect(message.getEpfdSuspect());
                return true;
            case EPFD_RESTORE:
                handleEpfdRestore(message.getEpfdRestore());
                return true;
        }
        return false;
    }

    /**
     * If a process is suspected to not be alive, it is added in the
     * suspected list and the new leader is updated.
     * @param epfdSuspect the message
     */
    private void handleEpfdSuspect(EpfdSuspect epfdSuspect) {
        if (!ProcessListUtils.contains(suspected, epfdSuspect.getProcess())) {
            suspected.add(epfdSuspect.getProcess());
            updateTrustedLeader();
        }
    }

    /**
     * If a process has been restored meanwhile, we quit suspecting it and we
     * reconsider to update the leader.
     * @param epfdRestore the message
     */
    private void handleEpfdRestore(EpfdRestore epfdRestore) {
        ProcessListUtils.remove(suspected, epfdRestore.getProcess());
        updateTrustedLeader();
    }

    /**
     * Updates the trusted leader if the current leader is not the the process with the maximum
     * rank within the nonsuspected processes.
     */
    private void updateTrustedLeader() {
        List<ProcessId> notSuspected = ProcessListUtils.difference(system.getProcessList(), suspected);
        if (notSuspected.isEmpty()) {
            return;
        }

        ProcessId maxRankProcessId = notSuspected.get(0);
        for (ProcessId processId : notSuspected) {
            if (processId.getRank() > maxRankProcessId.getRank()) {
                maxRankProcessId = processId;
            }
        }

        if (leader == null || leader.getRank() != maxRankProcessId.getRank()) {
            leader = maxRankProcessId;
            system.trigger(Message.newBuilder()
                    .setType(ELD_TRUST)
                    .setEldTrust(EldTrust.newBuilder()
                            .setProcess(leader)
                            .build())
                    .build());
        }
    }
}
