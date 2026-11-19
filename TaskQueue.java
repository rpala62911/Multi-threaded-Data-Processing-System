// Class manages the shared queue of ride requests. A LinkedBlockingQueue provides thread-safe task storage and retrieval. 
// Multiple worker threads can safely access the queue without creating race conditions.


 // Managing the shared queue of ride requests using a thread-safe blocking queue

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// Creating shared task queue class
public class TaskQueue {

    // Creating thread-safe shared blocking queue
    private final BlockingQueue<Task> taskQueue =
            new LinkedBlockingQueue<>();

    // Adding ride requests into the shared queue
    public void addTask(Task task) {
        taskQueue.add(task);
    }

    // Retrieving the next available ride request
    public Task getNextTask() throws InterruptedException {
        return taskQueue.take();
    }

    // Adding termination signals for workers
    public void stopWorkers(int numberOfWorkers) {

        for (int i = 0; i < numberOfWorkers; i++) {
            taskQueue.add(Task.createStopTask());
        }
    }
}
