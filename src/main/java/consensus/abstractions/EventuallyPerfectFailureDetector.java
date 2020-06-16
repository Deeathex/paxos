package consensus.abstractions;

import consensus.system.ConsensusSystem;
import consensus.utils.ProcessListUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static consensus.Paxos.*;
import static consensus.Paxos.Message.Type.*;

/**
 * An eventually perfect failure-detector abstraction that detects crashes
 * accurately after some a priori unknown point in time, but may make mistakes before
 * that time. This captures the intuition that, most of the time, timeout delays can be
 * adjusted so they can lead to accurately detecting crashes. However, there are periods
 * where the asynchrony of the underlying system prevents failure detection to be accurate
 * and leads to false suspicions. In this case, we talk about failure suspicion instead of
 * failure detection. In this case a timeout is used and the processes that do not send
 *  a heartbeat reply within the timeout delay are suspected to have crashed. If the timeout
 *  is too short, some processes that did not actually crashed may be suspected, so the timeout
 *  should be increased. The bound on the communication delay is not known, but it is sure that
 *  it will be one.
 */
public class EventuallyPerfectFailureDetector implements Abstraction {
    private static final String EPFD = "epfd";
    private static final int DELTA = 100;

    private ConsensusSystem system;
    private int delay = DELTA;
    private final List<ProcessId> alive = new ArrayList<>();
    private final List<ProcessId> suspected = new ArrayList<>();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public EventuallyPerfectFailureDetector(ConsensusSystem system) {
        this.system = system;
        this.alive.addAll(system.getProcessList());
        scheduleTimeout();
    }

    /**
     * Schedule the delay/timeout in which the heartbeat replies should be received
     */
    private void scheduleTimeout() {
        executorService.schedule(() -> {
            system.trigger(Message.newBuilder()
                    .setType(EPFD_TIMEOUT)
                    .setEpfdTimeout(EpfdTimeout.newBuilder()
                            .build())
                    .build());
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
                switch (message.getPlDeliver().getMessage().getType()) {
                    case EPFD_HEARTBEAT_REQUEST:
                        handleEpfdHeartbeatRequest(message.getPlDeliver().getSender());
                        return true;
                    case EPFD_HEARTBEAT_REPLY:
                        handleEpfdHeartbeatReply(message.getPlDeliver().getSender());
                        return true;
                }
                return false;
            case EPFD_TIMEOUT:
                handleEpfdTimeout();
                return true;
        }
        return false;
    }

    /**
     * If alive, send heartbeat reply to heartbeat request from processId
     *
     * @param processId the process that sent the heartbeat request
     */
    private void handleEpfdHeartbeatRequest(ProcessId processId) {
        system.trigger(Message.newBuilder()
                .setType(PL_SEND)
                .setAbstractionId(EPFD)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(processId)
                        .setMessage(Message.newBuilder()
                                .setType(EPFD_HEARTBEAT_REPLY)
                                .setEpfdHeartbeatReply(EpfdHeartbeatReply_.newBuilder()
                                        .build())
                                .build())
                        .build())
                .build());
    }

    /**
     * If a process responded, then consider it alive
     *
     * @param processId the process that responded with a heartbeat reply
     */
    private void handleEpfdHeartbeatReply(ProcessId processId) {
        if (!ProcessListUtils.contains(alive, processId)) {
            alive.add(processId);
        }
    }

    /**
     * Check if there is any process that was suspected to be not alive and now is alive.
     * If the delay is too small and not all processes managed to respond with a heartbeat reply, then increase it
     * to permit to all the processes that are alive to sent a heartbeat response.
     * Afterwards request a heartbeat reply from every processId.
     */
    private void handleEpfdTimeout() {
        //
        boolean intersectionEmpty = true;
        for (ProcessId aliveProcess : alive) {
            for (ProcessId suspectedProcess : suspected) {
                if (aliveProcess.getPort() == suspectedProcess.getPort()) {
                    intersectionEmpty = false;
                    break;
                }
            }
            if (!intersectionEmpty) {
                delay += DELTA;
                break;
            }
        }
        for (ProcessId processId : system.getProcessList()) {
            if (!ProcessListUtils.contains(alive, processId) && !ProcessListUtils.contains(suspected, processId)) {
                suspected.add(processId);
                system.trigger(Message.newBuilder()
                        .setType(EPFD_SUSPECT)
                        .setEpfdSuspect(EpfdSuspect.newBuilder()
                                .setProcess(processId)
                                .build())
                        .build());
            } else if (ProcessListUtils.contains(alive, processId) && ProcessListUtils.contains(suspected, processId)) {
                ProcessListUtils.remove(suspected, processId);
                system.trigger(Message.newBuilder()
                        .setType(EPFD_RESTORE)
                        .setEpfdRestore(EpfdRestore.newBuilder()
                                .setProcess(processId)
                                .build())
                        .build());
            }

            system.trigger(Message.newBuilder()
                    .setType(PL_SEND)
                    .setAbstractionId(EPFD)
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(processId)
                            .setMessage(Message.newBuilder()
                                    .setType(EPFD_HEARTBEAT_REQUEST)
                                    .setEpfdHeartbeatRequest(EpfdHeartbeatRequest_.newBuilder()
                                            .build())
                                    .build())
                            .build())
                    .build());
        }

        alive.clear();
        scheduleTimeout();
    }
}
