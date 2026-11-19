// Class represents individual ride requests in the ride-sharing system. Each task stores a unique identification number and pickup-to-drop-off information. 
// A poison-pill task is also included to signal workers to terminate safely after completing their assigned work.

 // Defining the task structure for storing ride request information
public class Task {

    // Declaring task identification
    private final int taskId;

    // Declaring ride request details
    private final String rideDetails;

    // Identifying termination tasks
    private final boolean poisonPill;

    // Creating constructor for normal ride requests
    public Task(int taskId, String rideDetails) {
        this.taskId = taskId;
        this.rideDetails = rideDetails;
        this.poisonPill = false;
    }

    // Creating constructor for worker termination
    private Task() {
        this.taskId = -1;
        this.rideDetails = "STOP";
        this.poisonPill = true;
    }

    // Creating termination task
    public static Task createStopTask() {
        return new Task();
    }

    // Returning task identification
    public int getTaskId() {
        return taskId;
    }

    // Returning ride request details
    public String getRideDetails() {
        return rideDetails;
    }

    // Checking whether the task is a termination signal
    public boolean isPoisonPill() {
        return poisonPill;
    }
}
