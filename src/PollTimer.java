import java.util.Timer;
import java.util.TimerTask;


/*
 * Summary
 * This class is a timer class.
 */
public class PollTimer{
    // Timer field.
    Timer timer;

    /*
     * Summary:
     * Class Ctor.
     */
    public PollTimer(int milliseconds, Scheduler callbackTarget) {
        // Create the timer as a daemon thread.  It will go away when the
        // last non-daemon process does. This will prevent having to flag
        // the timer for exit.
        timer = new Timer(true);

        timer.schedule(new RemindTask(callbackTarget), milliseconds);
	}


    /*
     * Summary:
     * This method kicks off the timer.
     */
    class RemindTask extends TimerTask {
        Scheduler sched_task;

        public RemindTask(Scheduler callbackTarget){
            sched_task = callbackTarget;
        }


        public void run() {
            System.out.format("[Poll Timer] Time's up!%n");
            sched_task.deadlockCheck();
            timer.cancel(); //Terminate the timer thread
        }
    }
}
