// The main Java program that creates ride requests and starts three worker threads using an ExecutorService. It coordinates task execution, manages the shared output file, and waits for worker termination. 
// The program also displays successful results and any errors encountered during processing.

 // Running the multi-threaded ride-sharing data processing system
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Creating main class for ride-sharing data processing system
public class DataProcessingSystem {

    // Starting program execution
    public static void main(String[] args) {

        // Defining number of worker threads
        int numberOfWorkers = 3;

        // Defining number of ride requests
        int numberOfTasks = 10;

        // Creating shared task queue
        TaskQueue taskQueue = new TaskQueue();

        // Adding ride requests into the queue
        for (int i = 1; i <= numberOfTasks; i++) {

            taskQueue.addTask(
                    new Task(
                            i,
                            "Pickup Location " + i
                                    + " to Drop Location " + i
                    )
            );
        }

        // Adding termination signals after all ride requests
        taskQueue.stopWorkers(numberOfWorkers);

        // Creating synchronized results collection
        List<String> results =
                Collections.synchronizedList(new ArrayList<>());

        // Creating synchronized error collection
        List<String> errors =
                Collections.synchronizedList(new ArrayList<>());

        // Creating shared file synchronization lock
        Object fileLock = new Object();

        // Creating fixed thread pool
        ExecutorService executor =
                Executors.newFixedThreadPool(numberOfWorkers);

        // Creating shared output file
        try (BufferedWriter writer =
                     new BufferedWriter(
                             new FileWriter("java_output.txt")
                     )) {

            // Starting worker threads
            for (int i = 1; i <= numberOfWorkers; i++) {

                executor.execute(
                        new Worker(
                                taskQueue,
                                results,
                                "Worker-" + i,
                                writer,
                                fileLock,
                                errors
                        )
                );
            }

            // Preventing additional task submissions
            executor.shutdown();

            try {

                // Waiting for worker threads to finish
                boolean completed = executor.awaitTermination(
                        2,
                        TimeUnit.MINUTES
                );

                // Handling workers that exceed the waiting period
                if (!completed) {

                    System.err.println(
                            "Processing timeout. Stopping workers."
                    );

                    executor.shutdownNow();

                    if (!executor.awaitTermination(
                            30,
                            TimeUnit.SECONDS
                    )) {
                        System.err.println(
                                "Some workers did not terminate."
                        );
                    }
                }

            } catch (InterruptedException e) {

                // Stopping workers after main thread interruption
                executor.shutdownNow();

                // Restoring interrupted status
                Thread.currentThread().interrupt();

                System.err.println(
                        "Main execution interrupted: "
                                + e.getMessage()
                );
            }

        } catch (IOException e) {

            // Handling output file creation and closing errors
            System.err.println(
                    "Output file error: " + e.getMessage()
            );

            executor.shutdownNow();
            return;
        }

        // Displaying final processing results
        System.out.println("\n========== FINAL RESULTS ==========");

        synchronized (results) {
            for (String result : results) {
                System.out.println(result);
            }
        }

        // Displaying processing summary
        System.out.println("\nTotal successful tasks: " + results.size());
        System.out.println("Total errors: " + errors.size());

        // Displaying recorded errors
        if (!errors.isEmpty()) {

            System.out.println("\n========== ERROR DETAILS ==========");

            synchronized (errors) {
                for (String error : errors) {
                    System.out.println(error);
                }
            }
        }

        // Displaying final completion message
        System.out.println(
                "\nRide-sharing data processing system finished."
        );
    }
}
