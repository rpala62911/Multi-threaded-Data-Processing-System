// Defines the worker threads responsible for processing ride requests. Each worker retrieves tasks from the shared queue, simulates processing time, and writes results to a shared output file. 
// Synchronization protects file access, while exception handling manages interruptions and writing errors.

 // Processing ride requests concurrently using multiple worker threads

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

// Creating worker class for processing ride requests
public class Worker implements Runnable {

    // Declaring shared task queue
    private final TaskQueue taskQueue;

    // Declaring shared results collection
    private final List<String> results;

    // Declaring worker identification
    private final String workerName;

    // Declaring shared output writer
    private final BufferedWriter writer;

    // Declaring file synchronization lock
    private final Object fileLock;

    // Declaring shared error collection
    private final List<String> errors;

    // Initializing worker information
    public Worker(
            TaskQueue taskQueue,
            List<String> results,
            String workerName,
            BufferedWriter writer,
            Object fileLock,
            List<String> errors
    ) {
        this.taskQueue = taskQueue;
        this.results = results;
        this.workerName = workerName;
        this.writer = writer;
        this.fileLock = fileLock;
        this.errors = errors;
    }

    // Starting worker execution
    @Override
    public void run() {

        System.out.println(workerName + " started.");

        try {

            // Retrieving and processing available tasks
            while (true) {

                Task task = taskQueue.getNextTask();

                // Stopping worker after receiving termination signal
                if (task.isPoisonPill()) {
                    break;
                }

                try {

                    // Displaying task processing message
                    System.out.println(
                            workerName + " processing Ride Request "
                                    + task.getTaskId()
                    );

                    // Simulating ride request processing delay
                    Thread.sleep(1000);

                    // Creating task processing result
                    String result = workerName
                            + " completed Ride Request "
                            + task.getTaskId()
                            + ": "
                            + task.getRideDetails();

                    // Writing result safely to shared output file
                    synchronized (fileLock) {

                        writer.write(result);
                        writer.newLine();
                        writer.flush();
                    }

                    // Storing successfully processed result
                    synchronized (results) {
                        results.add(result);
                    }

                    // Displaying task completion message
                    System.out.println(result);

                } catch (InterruptedException e) {

                    // Recording worker interruption
                    Thread.currentThread().interrupt();

                    synchronized (errors) {
                        errors.add(
                                workerName + " interrupted while processing "
                                        + task.getTaskId()
                        );
                    }

                    break;

                } catch (IOException e) {

                    // Recording file writing errors
                    synchronized (errors) {
                        errors.add(
                                "File writing error for Ride Request "
                                        + task.getTaskId()
                                        + ": "
                                        + e.getMessage()
                        );
                    }

                    System.err.println(
                            "Error writing Ride Request "
                                    + task.getTaskId()
                                    + ": "
                                    + e.getMessage()
                    );

                } catch (Exception e) {

                    // Recording unexpected processing errors
                    synchronized (errors) {
                        errors.add(
                                "Processing error for Ride Request "
                                        + task.getTaskId()
                                        + ": "
                                        + e.getMessage()
                        );
                    }

                    System.err.println(
                            "Processing error: " + e.getMessage()
                    );
                }
            }

        } catch (InterruptedException e) {

            // Handling interruption during queue retrieval
            Thread.currentThread().interrupt();

            synchronized (errors) {
                errors.add(workerName + " interrupted while waiting for tasks.");
            }

        } finally {

            // Displaying worker termination message
            System.out.println(workerName + " stopped.");
        }
    }
}
