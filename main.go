
package main

import (
    "fmt"
    "os"
    "sync"
    "time"
)

// Defining ride request information
type Task struct {
    taskID      int
    rideDetails string
}

// Processing ride requests using multiple goroutines
func worker(
    workerID int,
    tasks <-chan Task,
    results chan<- string,
    errors chan<- string,
    file *os.File,
    fileMutex *sync.Mutex,
    wg *sync.WaitGroup,
) {

    // Completing worker coordination when the function returns
    defer wg.Done()

    // Logging worker termination
    defer fmt.Printf("Worker-%d stopped.\n", workerID)

    // Retrieving ride requests until the task channel closes
    for task := range tasks {

        fmt.Printf(
            "Worker-%d started Task %d\n",
            workerID,
            task.taskID,
        )

        // Simulating ride request processing
        time.Sleep(1 * time.Second)

        result := fmt.Sprintf(
            "Task %d processed by Worker-%d - %s",
            task.taskID,
            workerID,
            task.rideDetails,
        )

        // Protecting shared file-writing operations
        fileMutex.Lock()

        _, err := file.WriteString(result + "\n")

        fileMutex.Unlock()

        // Checking file-writing errors
        if err != nil {
            errors <- fmt.Sprintf(
                "Task %d file error: %v",
                task.taskID,
                err,
            )
            continue
        }

        // Sending successful processing results
        results <- result

        fmt.Printf(
            "Worker-%d completed Task %d\n",
            workerID,
            task.taskID,
        )
    }
}

func main() {

    numberOfWorkers := 3
    numberOfTasks := 10

    // Creating the output file
    file, err := os.Create("go_output.txt")

    if err != nil {
        fmt.Println("File creation error:", err)
        return
    }

    // Creating shared channels
    tasks := make(chan Task, numberOfTasks)
    results := make(chan string, numberOfTasks)
    errors := make(chan string, numberOfTasks)

    // Creating synchronization mechanisms
    var wg sync.WaitGroup
    var fileMutex sync.Mutex

    // Starting worker goroutines
    for i := 1; i <= numberOfWorkers; i++ {

        wg.Add(1)

        go worker(
            i,
            tasks,
            results,
            errors,
            file,
            &fileMutex,
            &wg,
        )
    }

    // Submitting ride requests
    for i := 1; i <= numberOfTasks; i++ {

        tasks <- Task{
            taskID: i,
            rideDetails: fmt.Sprintf(
                "Pickup Point %d to Drop-off Point %d",
                i,
                i,
            ),
        }
    }

    // Closing the task channel after submission
    close(tasks)

    // Waiting for all workers to finish
    wg.Wait()

    // Closing the output file after worker completion
    closeErr := file.Close()

    if closeErr != nil {
        fmt.Println("File closing error:", closeErr)
    }

    // Closing result and error channels
    close(results)
    close(errors)

    // Collecting processing results
    var processedResults []string

    for result := range results {
        processedResults = append(processedResults, result)
    }

    // Collecting processing errors
    var processingErrors []string

    for processingError := range errors {
        processingErrors = append(
            processingErrors,
            processingError,
        )
    }

    // Displaying the final processing summary
    fmt.Println("\nGO PROCESSING SUMMARY")
    fmt.Println("---------------------")
    fmt.Println("Total tasks:", numberOfTasks)
    fmt.Println("Workers:", numberOfWorkers)
    fmt.Println("Successful tasks:", len(processedResults))
    fmt.Println("Errors:", len(processingErrors))

    fmt.Println("\nProcessed Results:")

    for _, result := range processedResults {
        fmt.Println(result)
    }

    if len(processingErrors) > 0 {

        fmt.Println("\nProcessing Errors:")

        for _, processingError := range processingErrors {
            fmt.Println(processingError)
        }
    }

    fmt.Println("\nGo output saved to go_output.txt")
}
