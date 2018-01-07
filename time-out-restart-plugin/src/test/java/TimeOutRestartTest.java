import org.junit.Before;
import org.junit.Test;
import utils.ShellCommandExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by eddie on 12/29/17.
 */
public class TimeOutRestartTest {
    String testAppId;
    String HADOOP_HOME;
    @Before
    public void setUp() throws Exception {
        //timeOutRestart = new TimeOutRestart("timeout", 1);
        HADOOP_HOME = "/home/eddie/hadoop-2.7.3";
        testAppId = "application_1514420769174_0017";
    }

    @Test
    public void getAppCommand() throws Exception {
        String appCommand = "";

        // find the type of the app
        ShellCommandExecutor executor = new ShellCommandExecutor(HADOOP_HOME + "/bin/yarn application -status " + testAppId);
        executor.execute();
        String[] lines = executor.getOutput().split("\n");
        if (lines.length < 10) {
            System.out.print("connect to RM failed or no such appId");
            return;
        }
        String appType = "";
        if (lines[2].matches(".*Application-Name.*")) {
            appType = lines[2].split(":")[1].trim().split("\\s+")[0].trim();
        }

        // find the start command of the app
        //executor = new ShellCommandExecutor("ps -aux | grep -n 'python2.*" + appType + "'");
        executor = new ShellCommandExecutor("ps -aux");
        executor.execute();
        lines = executor.getOutput().split("\n");
        String targetLine = null;
        for (String line: lines) {
            if (line.matches(".*python2.*" + appType + ".*")) {
                targetLine = line;
                break;
            }
        }
        if (targetLine != null) {
            appCommand = targetLine.substring(targetLine.indexOf("python2"));
        }

        System.out.printf("app command: %s\n", appCommand);
    }

    @Test
    public void startApp() throws Exception {
        String startCommand = "/home/eddie/lib/jdk1.8.0_111/bin/java -cp /home/eddie/spark-2.1.0/conf/:/home/eddie/spark-2.1.0/jars/*:/home/eddie/hadoop-2.7.3/etc/hadoop/ -Xmx1g org.apache.spark.deploy.SparkSubmit --master yarn --properties-file /home/eddie/HiBench/report/wordcount/spark/conf/sparkbench/spark.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount /home/eddie/HiBench/sparkbench/assembly/target/sparkbench-assembly-6.0-SNAPSHOT-dist.jar hdfs://192.168.32.111:9000/HiBench/Wordcount/Input hdfs://192.168.32.111:9000/HiBench/Wordcount/Output";
        List<String> commands = new ArrayList<>();
        ProcessBuilder pb;
        commands.add("/bin/bash");
        commands.add("-c");
        commands.add(startCommand);
        pb = new ProcessBuilder(commands);
        pb.environment().put("YARN_CONF_DIR", "/home/eddie/hadoop-2.7.3/etc/hadoop");
        pb.environment().put("SCALA_HOME", "/home/eddie/lib/scala-2.11.8");
        pb.environment().put("PATH", "$PATH:$SCALA_HOME/bin");
        pb.environment().put("SPARK_HOME", "/home/eddie/spark-2.1.0");
        //pb.redirectError(new File("/dev/null"));
        //pb.redirectInput(new File("/dev/null"));

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
    }

    @Test
    public void findAppType() throws Exception {
        String appId = "application_1515094056290_0072";
        ShellCommandExecutor executor = new ShellCommandExecutor(HADOOP_HOME + "/bin/yarn application -status " + appId);
        executor.execute();
        System.out.printf("got app status\n");
        String[] lines = executor.getOutput().split("\n");
        if (lines.length < 10) {
            System.out.print("connect to RM failed or no such appId\n");
            return;
        }
        String appType = "";
        System.out.printf("line 2: %s\n", lines[2]);
        if (lines[2].matches(".*Application-Name.*")) {
            appType = lines[2].split(":")[1].trim().split("\\s")[0];
        }
        System.out.printf("app type: %s\n", appType);
        if (appType.equals("")) {
            return;
        }
    }

}