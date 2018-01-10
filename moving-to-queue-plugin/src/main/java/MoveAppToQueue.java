import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import server.TracerConf;
import utils.ShellCommandExecutor;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MoveAppToQueue extends AbstractFeedback {

    TracerConf conf = TracerConf.getInstance();
    int timeoutThreshold;
    final Double CLUSTER_MEMORY = 64 * 1024 * 1024 * 1024D;
    final int minimumMovingInterval = 10;
    int lastMovingTime = minimumMovingInterval;
    Boolean isMoving = false;
    ConcurrentMap<String, Integer> pendingAppCounterMap;
    ConcurrentMap<String, Integer> underUtilizedAppCounterMap;
    Map<String, Double> appMemoryUsage;
    Map<String, Double> appStateMap;
    Map<String, Integer> appThreshold;
    String appMoving = "";
    List<SchedulerQueue> leafQueues;

    public MoveAppToQueue(String name, Integer interval) {
        super(name, interval);
        timeoutThreshold = conf.getIntegerOrDefault("tracer.plugin.timeout.threshold", 10);
        pendingAppCounterMap = new ConcurrentHashMap<>();
        underUtilizedAppCounterMap = new ConcurrentHashMap<>();
        appMemoryUsage = new HashMap<>();
        appStateMap = new HashMap<>();
        appThreshold = new HashMap<>();
        leafQueues = new LinkedList<>();
        initQueues();
        System.out.print("MoveAppToQueue plugin is loaded\n");
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        // Find "app.state" message from this window. Then, app/remove the corresponding entry in the pendingAppCounterMap.
        Map<String, Double> currentMemoryUsage = new HashMap<>();
        for(Map<String, AnalysisContainer> containerMap: list) {
            for(Map.Entry<String, AnalysisContainer> entry: containerMap.entrySet()) {
                String id = entry.getKey();
                AnalysisContainer value = entry.getValue();

                // if the app is stuck in ACCEPTED state,
                // add it to the pending queue
                if (id.matches("application.*")) {
                    updateAppState(id, value);
                }


                // update the app memory usages in this window.
                String containerId = entry.getKey();
                if (containerId.matches("container.*")) {
                    if (appStateMap.containsKey(containerToAppId(containerId))) {
                        String appId = containerToAppId(containerId);
                        currentMemoryUsage.putIfAbsent(appId, 0D);
                        Double memoryUsage = entry.getValue().memory;
                        Double memorySum = currentMemoryUsage.get(appId);
                        currentMemoryUsage.put(appId, memorySum + memoryUsage);
                    }
                }
            }
        }

        // Update the pendingAppCounterMap periodically
        updatePendingApp();

        // Update the underUtilizedMap periodically
        updateUnderUtilizedApp(currentMemoryUsage);

        // TEST
        printCounter();

        maybeMoveApp();
    }

    private void updateAppState(String appId, AnalysisContainer container) {
        List<KeyedMessage> messageList = container.instantMessages.get("app.state");
        if (messageList != null) {
            for (KeyedMessage message : messageList) {
                Double doubleValue = message.value;
                String messageKey = message.key;
                if (messageKey.equals("app.state")) {
                    System.out.printf("get app state log. app: %s, state: %f\n", appId, doubleValue);
                    if (Double.compare(doubleValue, 5D) <= 0) {
                        appStateMap.put(appId, doubleValue);
                        appThreshold.putIfAbsent(appId, timeoutThreshold);

                        if (Double.compare(doubleValue, 5D) == 0) {
                            appMemoryUsage.putIfAbsent(appId, 0D);
                        }
                    } else {
                        appStateMap.remove(appId);
                        appMemoryUsage.remove(appId);
                        underUtilizedAppCounterMap.remove(appId);
                        appThreshold.remove(appId);
                    }
                    int compareResult = Double.compare(doubleValue, 4);
                    if (compareResult == 1) {
                        System.out.printf("remove app: %s from pending.\n", appId);
                        pendingAppCounterMap.remove(appId);
                    } else if (compareResult == 0) {
                        System.out.printf("move app: %s to pending.\n", appId);
                        pendingAppCounterMap.put(appId, 0);
                    }
                }
            }
        }
    }

    private void updatePendingApp() {
        for (String id: pendingAppCounterMap.keySet()) {
            Double appState = appStateMap.get(id);
            if (appState != null) {
                if (Double.compare(appState, 4d) <= 0) {
                    pendingAppCounterMap.compute(id, (key, value) -> value + 1);
                } else {
                    pendingAppCounterMap.remove(id);
                }
            } else {
                pendingAppCounterMap.remove(id);
            }

        }
    }

    private void updateUnderUtilizedApp(Map<String, Double> currentMemoryMap) {
        for (Map.Entry<String, Double> entry: currentMemoryMap.entrySet()) {
            String appId = entry.getKey();
            Double newUsage = entry.getValue();
            Double usage = appMemoryUsage.get(appId);
            if (usage != null && !appId.equals(appMoving)) {
                if (usage < CLUSTER_MEMORY * 0.5) {
                    if (newUsage - usage >= -104857600D && (newUsage - usage) < 52428800D) {
                        underUtilizedAppCounterMap.compute(appId, (key, value) -> value == null ? 0 : value + 1);
                    } else {
                        underUtilizedAppCounterMap.compute(appId, (key, value) -> value == null ? 0 : 0);
                    }
                }
            }
            if (appId.equals(appMoving)) {
                underUtilizedAppCounterMap.remove(appId);
            }
        }
        appMemoryUsage.putAll(currentMemoryMap);
    }

    private void maybeMoveApp() {
        lastMovingTime += 1;
        boolean isPendingApp = true;
        if (lastMovingTime <= minimumMovingInterval) {
            return;
        }
        String appToMove = null;
        for (Map.Entry<String, Integer> counterEntry: pendingAppCounterMap.entrySet()) {
            if (counterEntry.getValue() > appThreshold.get(counterEntry.getKey())) {
                appToMove = counterEntry.getKey();
                break;
            }
        }

        if (appToMove != null) {
            pendingAppCounterMap.remove(appToMove);
        } else {
            // if no app is in ACCEPTED state,
            // we further find if any app that is under-utilized.
            for (Map.Entry<String, Integer> counterEntry: underUtilizedAppCounterMap.entrySet()) {
                if (counterEntry.getValue() > appThreshold.get(counterEntry.getKey())) {
                    appToMove = counterEntry.getKey();
                    break;
                }
            }
            if (appToMove != null) {
                isPendingApp = false;
                underUtilizedAppCounterMap.remove(appToMove);
            }
        }

        if (appToMove != null && !isMoving) {
            appThreshold.computeIfPresent(appToMove, (key, value) -> value * 2);
            Thread movingAppThread = new Thread(new MoveAppRunnable(appToMove, isPendingApp));
            movingAppThread.start();
            lastMovingTime = 0;
        }
    }



    private void initQueues() {
        leafQueues.add(new SchedulerQueue("default", true));
        leafQueues.add(new SchedulerQueue("alpha", true));
        //leafQueues.add(new SchedulerQueue("beta", true));
        //leafQueues.add(new SchedulerQueue("gama", true));
    }



    public class SchedulerQueue {
        public String name;
        public Boolean isLeaf;
        public Double remainingCapacity;

        public SchedulerQueue(String name, Boolean isLeaf) {
            this.name = name;
            this.isLeaf = isLeaf;
            remainingCapacity = 0d;
        }
    }

    private String containerToAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = "application_" + parts[1] + "_" + parts[2];

        return appId;
    }

    /**
     * This method is for test only
     */
    private void printCounter() {
        if (pendingAppCounterMap.isEmpty() && underUtilizedAppCounterMap.isEmpty()) {
            return;
        }
        System.out.print("***\n");
        System.out.print("pending apps:\n");
        for(Map.Entry<String, Integer> entry: pendingAppCounterMap.entrySet()) {
            System.out.printf("app: %s, count: %d\n", entry.getKey(), entry.getValue());
        }
        System.out.print("under-until apps:\n");
        for(Map.Entry<String, Integer> entry: underUtilizedAppCounterMap.entrySet()) {
            System.out.printf("app: %s, count: %d\n", entry.getKey(), entry.getValue());
        }
        System.out.print("app memory usage\n");
        for(Map.Entry<String, Double> entry: appMemoryUsage.entrySet()) {
            System.out.printf("app: %s, memory: %f\n", entry.getKey(), entry.getValue() / 1024 / 1024);
        }
    }

    // public for test
    public class MoveAppRunnable implements Runnable {
        String appId;
        boolean isPendingApp;

        public MoveAppRunnable(String appId, boolean isPendingApp) {
            this.appId = appId;
            appMoving = appId;
            this.isPendingApp = isPendingApp;
            isMoving = true;

        }

        @Override
        public void run() {
            System.out.printf("start moving app: %s\n", appId);
            moveApp();
            isMoving = false;
            if (appStateMap.containsKey(appId)) {
                if (isPendingApp && appStateMap.get(appId) < 5d) {
                    pendingAppCounterMap.put(appId, 0);
                }
            }
            appMoving = "";
        }

        private void moveApp() {
            String queueToMove = findOptimalQueue();
            System.out.printf("Moving App: %s to queue: %s\n", appId, queueToMove);

            String command = "/home/eddie/hookup/script/move-app-to-queue.sh " + appId + " " + queueToMove;
            ShellCommandExecutor executor = new utils.ShellCommandExecutor(command);
            try {
                executor.execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String result = executor.getOutput();
            if (result.matches(".*successfully.*")) {
                System.out.printf("Successfully moved App: %s to queue: %s\n", appId, queueToMove);
            } else {
                System.out.printf("%s\n", result);
            }
        }

        /**
         * execute shell script to find the available queue.
         * public for test
         *
         * @return the name of the available queue.
         */
        public String findOptimalQueue() {
            String optimalQueueName = "";
            double optimalRemaining = -10d;
            for (SchedulerQueue queue: leafQueues) {
                queue.remainingCapacity = getQueueRemainingCapacity(queue.name);
                if (optimalRemaining < queue.remainingCapacity) {
                    optimalRemaining = queue.remainingCapacity;
                    optimalQueueName = queue.name;
                }
            }
            return optimalQueueName;
        }

        private Double getQueueRemainingCapacity(String name) {
            String command = "/home/eddie/hadoop-2.7.3/bin/yarn queue -status " + name;
            ShellCommandExecutor executor = new utils.ShellCommandExecutor(command);
            try {
                executor.execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String result = executor.getOutput();
            String[] resultLines = result.split("\\n");
            if (resultLines.length != 8) {
                isRunning = false;
                return -1d;
            }
            double capacity = parseCapacityFromLine(resultLines[3]);
            double current = parseCapacityFromLine(resultLines[4]) / 100;
            double maximum = parseCapacityFromLine(resultLines[5]);
            double remaining = maximum - capacity * current;

            return remaining;

        }

        private double parseCapacityFromLine(String line) {
            String doubleStr = line.split(":")[1].replace("%", "").trim();
            double value = Double.valueOf(doubleStr);
            return value;
        }
    }
}
