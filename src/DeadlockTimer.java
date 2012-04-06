import java.util.Timer;
import java.util.TimerTask;


/*
 * Summary
 * This class is a timer class.
 */
public class DeadlockTimer {
    // Timer field.
    Timer timer;

    /*
     * Summary:
     * Class Ctor.
     */
    public void MyTimer(int seconds, Scheduler callbackTarget) {
        timer = new Timer();
        timer.schedule(new RemindTask(), seconds*1000);
	}


    /*
     * Summary:
     * This method kicks off the timer.
     */
    class RemindTask extends TimerTask {
        public void run() {
            // TODO: (goldswjm) Change this to a call in the scheduler.
            System.out.format("Time's up!%n");
            timer.cancel(); //Terminate the timer thread
        }
    }
}
