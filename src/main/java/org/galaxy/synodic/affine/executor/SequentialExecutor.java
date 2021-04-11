package org.galaxy.synodic.affine.executor;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.galaxy.synodic.affine.executor.GroupingTask.*;

@Slf4j
public class SequentialExecutor implements GroupingExecutor {

    private final ExecutorService                                 executor;
    private final Map<Comparable<?>, GroupingTask>                groupTaskMap;
    private final Function<Comparable<?>, ? extends GroupingTask> groupFactory;
    private final OverFlowHandler                                 overFlowHandler;


    public SequentialExecutor(ExecutorService executor, Map<Comparable<?>, GroupingTask> groupTaskMap, Function<Comparable<?>, ? extends GroupingTask> groupFactory, OverFlowHandler overFlowHandler) {
        this.groupTaskMap = groupTaskMap;
        this.executor = executor;
        this.groupFactory = groupFactory;
        this.overFlowHandler = overFlowHandler;
    }

    public SequentialExecutor(ExecutorService executor, Function<Comparable<?>, GroupingTask> groupFactory) {
        this.groupTaskMap = new ConcurrentHashMap<>();
        this.executor = executor;
        this.groupFactory = groupFactory;
        this.overFlowHandler = new DiscardOldest();
    }

    public SequentialExecutor(ExecutorService executor) {
        this(executor, null);
    }

    @Override
    public void execute(@NonNull Comparable<?> key, @NonNull Runnable task) {
        if (executor.isShutdown()) {
            overFlowHandler.onOverFlow(key, task, this);
        }
        GroupingTask group = getOrCreateGroup(key);
        long expected = group.getVersion();
        boolean added = group.getSubTasks().offer(task);
        if (added) {
            for (; ; expected = group.getVersion()) {
                if ((expected & IN) == OUT && group.casVersion(expected, expected + ENTER)) {
                    executor.execute(group);
                    break;
                } else if ((expected & IN) == IN && group.casVersion(expected, expected + UPDATE)) {
                    break;
                }
            }
        } else {
            overFlowHandler.onOverFlow(key, task, this);
        }
    }

    private GroupingTask getOrCreateGroup(Comparable<?> key) {
        GroupingTask groupingTask = groupTaskMap.get(key);
        if (groupingTask == null) {
            if (groupFactory != null) {
                groupingTask = groupTaskMap.computeIfAbsent(key, groupFactory);
            } else {
                throw new IllegalArgumentException("group queue not found");
            }
        }
        return groupingTask;
    }

    public GroupingTask putGroupIfAbsent(Comparable<?> key, GroupingTask groupingTask) {
        return this.groupTaskMap.putIfAbsent(key, groupingTask);
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public void shutdown() {
        executor.shutdown();
    }

    public class DiscardOldest implements OverFlowHandler {
        @Override
        public void onOverFlow(Comparable<?> key, Runnable task, SequentialExecutor executor) {
            GroupingTask group = executor.groupTaskMap.get(key);
            group.getSubTasks().poll();
            executor.execute(key, task);
        }
    }

    public interface OverFlowHandler {
        void onOverFlow(Comparable<?> key, Runnable task, SequentialExecutor executor);
    }

}
