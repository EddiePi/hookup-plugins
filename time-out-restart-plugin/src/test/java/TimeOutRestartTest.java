import org.junit.Before;
import org.junit.Test;
import utils.ShellCommandExecutor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by eddie on 12/29/17.
 */
public class TimeOutRestartTest {
    String testAppId;
    String yarnHome;
    @Before
    public void setUp() throws Exception {
        //timeOutRestart = new TimeOutRestart("timeout", 1);
        yarnHome = "/home/eddie/hadoop-2.7.3/bin/yarn";
        testAppId = "application_1514420769174_0017";
    }

    @Test
    public void getAppCommand() throws Exception {
        String appCommand = "";

        // find the type of the app
        ShellCommandExecutor executor = new ShellCommandExecutor(yarnHome + " application -status " + testAppId);
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
    public void killApp() throws Exception {

    }

}