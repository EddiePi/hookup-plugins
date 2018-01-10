import detection.AnalysisContainer;
import detection.KeyedMessage;
import feedback.AbstractFeedback;
import server.TracerConf;
import utils.ShellCommandExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimeOutRestart extends AbstractFeedback {

    Map<String, Double> appState;
    Map<String, Integer> containerTimeoutMap;
    private String HADOOP_HOME = "/home/eddie/hadoop-2.7.3";
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    TracerConf conf = TracerConf.getInstance();
    Integer idleTimeout;
    Set<String> killedApp;

    public TimeOutRestart(String name, Integer interval) {
        super(name, interval);
        appState = new HashMap<>();
        containerTimeoutMap = new HashMap<>();
        idleTimeout = conf.getIntegerOrDefault("tracer.plugin.timeout.threshold", 10);
        killedApp = new HashSet<>();
        System.out.printf("TimeOutRestart plugin started. threshold: %d\n", idleTimeout);
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        for (Map<String, AnalysisContainer> containerMap: list) {
            for (Map.Entry<String, AnalysisContainer> entry: containerMap.entrySet()) {
                // this loop layer is for container
                String id = entry.getKey();
                AnalysisContainer value = entry.getValue();

                if (id.matches("application.*")) {
                    List<KeyedMessage> messageList = value.instantMessages.get("app.state");
                    if (messageList != null) {
                        for (KeyedMessage message : messageList) {
                            Double doubleValue = message.value;
                            String messageKey = message.key;
                            if (messageKey.equals("app.state")) {
                                System.out.printf("get app state: %f\n", doubleValue);
                                if (Double.compare(doubleValue, 5) == 1) {
                                    appState.remove(id);
                                    removeAllContainerOfApp(id);
                                } else if (Double.compare(doubleValue, 5) == 0) {
                                    appState.put(id, doubleValue);
                                    //containerTimeoutMap.put(id, 0);
                                }
                            }
                        }
                    }
                }

                // if we receive a new log message of a container, reset the count

                if (id.matches("container.*") && !id.matches(".*_000001")) {
                    String appId = containerToAppId(id);
                    if (!killedApp.contains(appId)) {
                        containerTimeoutMap.put(id, 0);
                    }
                }

                maybeRestartApplications();
            }
        }

        // add 1 to all container id count
        Set<String> containerKeySet = new HashSet<>(containerTimeoutMap.keySet());
        for (String containerId : containerKeySet) {
            Integer count = containerTimeoutMap.get(containerId) + 1;
            containerTimeoutMap.put(containerId, count);
        }
    }

    private String containerToAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = "application_" + parts[1] + "_" + parts[2];

        return appId;
    }

    private void maybeRestartApplications() {
        Set<String> appToRestart = new HashSet<>();
        for (Map.Entry<String, Integer> entry: containerTimeoutMap.entrySet()) {
            if (entry.getValue() > idleTimeout) {
                String appId = containerToAppId(entry.getKey());
                System.out.printf("add app to restart: %s\n", appId);
                appToRestart.add(appId);
            }
        }
        for (String appId: appToRestart) {
            String appStartCommand = "";
            String outputDir;
            try {
                appStartCommand = getAppCommand(appId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(appStartCommand.equals("") || appStartCommand == null) {
                return;
            }
            outputDir = getAppOutputDir(appStartCommand);
            String realCommand = appStartCommand;
            if (outputDir != null) {
                realCommand = HADOOP_HOME + "/bin/hadoop fs -rm -r " + outputDir + " && " + realCommand;
            }

            // kill the app before we restart it again.
            killApp(appId);
            removeAllContainerOfApp(appId);
            killedApp.add(appId);

            System.out.printf("restarting app: %s\n", appId);
            executorService.execute(new AppExecRunnable(realCommand));
        }
    }

    private class AppExecRunnable implements Runnable {

        List<String> commands = new ArrayList<>();
        ProcessBuilder pb;
        public AppExecRunnable(String startCommand) {
            commands.add("/bin/bash");
            commands.add("-c");
            commands.add(startCommand);
            pb = new ProcessBuilder(commands);
            pb.environment().put("YARN_CONF_DIR", HADOOP_HOME + "/etc/hadoop");
            pb.environment().put("SCALA_HOME", "/home/eddie/lib/scala-2.11.8");
            pb.environment().put("PATH", "$PATH:$SCALA_HOME/bin");
            pb.environment().put("SPARK_HOME", "/home/eddie/spark-2.1.0");
            //pb.redirectError(new File("/dev/null"));
            //pb.redirectInput(new File("/dev/null"));
        }

        @Override
        public void run() {
            System.out.printf("starting app. cmd: %s\n", commands.get(2));
            try {
                Process p = pb.start();
                BufferedReader brInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader brError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                String line;
                while ((line = brInput.readLine()) != null) {
                    System.out.println(line);
                }
                while ((line = brError.readLine()) != null) {
                    System.out.println(line);
                }
                p.waitFor();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.printf("restarted app done.\n");
        }
    }

    // public for test
    public String getAppCommand(String appId) throws IOException {
        String appCommand = "";
        System.out.printf("getting app command of %s\n", appId);

        // find the type of the app
        ShellCommandExecutor executor = new ShellCommandExecutor(HADOOP_HOME + "/bin/yarn application -status " + appId);
        executor.execute();
        System.out.printf("got app status\n");
        String[] lines = executor.getOutput().split("\n");
        if (lines.length < 10) {
            System.out.print("connect to RM failed or no such appId\n");
            return "";
        }
        String appType = "";
        if (lines[2].matches(".*Application-Name.*")) {
            appType = lines[2].split(":")[1].trim().split("\\s")[0];
        }
        System.out.printf("app type: %s\n", appType);
        if (appType.equals("")) {
            return "";
        }

        System.out.print("getting start command.\n");
        executor = new ShellCommandExecutor("ps -aux");
        executor.execute();
        lines = executor.getOutput().split("\n");
        String targetLine = null;
        for (String line: lines) {
            if (line.matches(".*/home/eddie/lib/jdk1.8.0_111/bin/java -cp.*spark.*" + appType + ".*")) {
                targetLine = line;
                break;
            }
        }
        if (targetLine != null) {
            appCommand = targetLine.substring(targetLine.indexOf("/home/eddie/lib/jdk1.8.0_111/bin/java"));
        }
        System.out.printf("app command: %s\n", appCommand);
        return appCommand;
    }

    private String getAppOutputDir(String startCommand) {
        String[] parts = startCommand.split("\\s+");
        String outputDir = parts[parts.length - 1];
        if (outputDir.toLowerCase().matches(".*output.*")) {
            return outputDir;
        }
        return null;
    }

    // public for test
    public void killApp(String appId) {
        System.out.printf("killing app: %s\n", appId);
        ShellCommandExecutor executor = new ShellCommandExecutor(HADOOP_HOME + "/bin/yarn application -kill " + appId);
        try {
            executor.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void removeAllContainerOfApp(String appId) {
        String[] parts = appId.split("_");
        String numericId = parts[1] + "_" + parts[2];
        Set<String> keySet = new HashSet<>(containerTimeoutMap.keySet());
        System.out.printf("in remove all con. numeric id: %s\n", numericId);
        for(String containerId: keySet) {
            if (containerId.matches(".*" + numericId + ".*")) {
                System.out.printf("deleting con: %s\n", containerId);
                containerTimeoutMap.remove(containerId);
            }
        }
    }
}
