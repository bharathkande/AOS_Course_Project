package pluradj.titan.tinkerpop3.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        Service service = Service.getInstance();
        try {
            logger.info("Sending request....");

            // should print marko, josh and peter to the logs
            service.step0();
            service.step1();
        //   service.step2();
        //    service.step3();
            //service.oldstep1();
           // service.degreecommon();
           // service.degree0();
           //service.degree1();
           // service.degree2();
            //service.degree3();

        } catch (Exception ex) {
            logger.error("Could not execute traversal", ex);
        } finally {
            service.close();
            logger.info("Service closed and resources released");
        }
    }
}
