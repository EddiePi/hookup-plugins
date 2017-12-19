import detection.AnalysisContainer;
import feedback.AbstractFeedback;

import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/12/12.
 */
public class TestPlugin extends AbstractFeedback {
    public TestPlugin(String name, Integer interval) {
        super(name, interval);
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> list) {
        for(int i = 0; i < 5; i++) {
            System.out.printf("this is test plugin, iter: %d\n", i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        isRunning = false;

    }
}
