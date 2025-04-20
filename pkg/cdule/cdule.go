package cdule

import (
    "encoding/json"
    "os"
    "reflect"
    "time"

    "github.com/deepaksinghvi/cdule/pkg/model"
    log "github.com/sirupsen/logrus"
    "gorm.io/gorm"
)

// WorkerID string
var WorkerID string

// Cdule holds watcher objects
type Cdule struct {
    *WorkerWatcher
    *ScheduleWatcher
}

func init() {
    WorkerID = getWorkerID()
}

// NewCduleWithWorker to create new scheduler with worker
func (cdule *Cdule) NewCduleWithWorker(workerName string, param ...string) {
    WorkerID = workerName
    cdule.NewCdule(param...)
}

// NewCdule to create new scheduler with default worker name as hostname
func (cdule *Cdule) NewCdule(param ...string) {
    if len(param) == 0 {
        param = []string{"./resources", "config", "errorLogType"} // default path for resources
    }
    _, err := model.ConnectDataBase(param)
    if err != nil {
        log.Errorf("Error getting configuration: %s", err.Error())
        return
    }

    // Register or update worker
    worker, err := model.CduleRepos.CduleRepository.GetWorker(WorkerID)
    if err != nil {
        log.Errorf("Error getting worker: %s", err.Error())
        return
    }
    if worker != nil {
        worker.UpdatedAt = time.Now()
        _, err = model.CduleRepos.CduleRepository.UpdateWorker(worker) // Handle two return values
        if err != nil {
            log.Errorf("Error updating worker: %s", err.Error())
        }
    } else {
        worker := model.Worker{
            WorkerID:  WorkerID,
            CreatedAt: time.Now(),
            UpdatedAt: time.Now(),
            DeletedAt: gorm.DeletedAt{},
        }
        _, err = model.CduleRepos.CduleRepository.CreateWorker(&worker) // Handle two return values
        if err != nil {
            log.Errorf("Error creating worker: %s", err.Error())
        }
    }

    // Initialize watchers and load jobs
    cdule.createWatcherAndWaitForSignal()
}

// createWatcherAndWaitForSignal initializes watchers and loads jobs
func (cdule *Cdule) createWatcherAndWaitForSignal() {
    workerWatcher := createWorkerWatcher()
    schedulerWatcher := createSchedulerWatcher()

    // Load existing jobs from database
    err := cdule.loadJobsFromDB(schedulerWatcher)
    if err != nil {
        log.Errorf("Error loading jobs from database: %s", err.Error())
    }

    cdule.WorkerWatcher = workerWatcher
    cdule.ScheduleWatcher = schedulerWatcher
}

// loadJobsFromDB loads and reschedules jobs from the database
func (cdule *Cdule) loadJobsFromDB(watcher *ScheduleWatcher) error {
    jobs, err := model.CduleRepos.CduleRepository.GetJobs() // Requires GetJobs in model
    if err != nil {
        return err
    }

    for _, job := range jobs {
        // Deserialize job data
        jobData := make(map[string]string)
        if err := json.Unmarshal([]byte(job.JobData), &jobData); err != nil {
            log.Errorf("Failed to deserialize job data for job %s: %s", job.JobName, err.Error())
            continue
        }

        // Create job instance
        jobInstance := createJobInstance(job.JobName)
        if jobInstance == nil {
            log.Errorf("Unknown job type: %s", job.JobName)
            continue
        }

        // Reschedule job
        _, err := watcher.NewJob(jobInstance, jobData).Build(job.CronExpression) // Requires NewJob in ScheduleWatcher
        if err != nil {
            log.Errorf("Failed to reschedule job %s: %s", job.JobName, err.Error())
            continue
        }
        log.Infof("Rescheduled job %s with schedule %s", job.JobName, job.CronExpression)
    }
    return nil
}

// StopWatcher to stop watchers
func (cdule *Cdule) StopWatcher() {
    if cdule.WorkerWatcher != nil {
        cdule.WorkerWatcher.Stop()
    }
    if cdule.ScheduleWatcher != nil {
        cdule.ScheduleWatcher.Stop()
    }
}

// createWorkerWatcher creates a worker health check watcher
func createWorkerWatcher() *WorkerWatcher {
    workerWatcher := &WorkerWatcher{
        Closed: make(chan struct{}),
        Ticker: time.NewTicker(time.Second * 30), // worker health check every 30 seconds
    }

    workerWatcher.WG.Add(1)
    go func() {
        defer workerWatcher.WG.Done()
        workerWatcher.Run()
    }()
    return workerWatcher
}

// createSchedulerWatcher creates a job scheduler watcher
func createSchedulerWatcher() *ScheduleWatcher {
    scheduleWatcher := &ScheduleWatcher{
        Closed: make(chan struct{}),
        Ticker: time.NewTicker(time.Minute * 1), // check schedules every minute
    }

    scheduleWatcher.WG.Add(1)
    go func() {
        defer scheduleWatcher.WG.Done()
        scheduleWatcher.Run()
    }()
    return scheduleWatcher
}

// createJobInstance creates a job instance based on job name
func createJobInstance(jobName string) Job {
    if t, exists := JobRegistry[jobName]; exists {
        return reflect.New(t).Elem().Interface().(Job)
    }
    return nil
}

// getWorkerID returns the hostname as worker ID
func getWorkerID() string {
    hostname, err := os.Hostname()
    if err != nil {
        log.Fatal("Failed to get hostname:", err)
    }
    return hostname
}