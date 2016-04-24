#include "TetrischedService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "YARNTetrischedService.h"
#include <thrift/transport/TSocket.h>                                                                                             
#include <thrift/transport/TTransportUtils.h>
#include <queue>
#include <set>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <fstream>
#include <sstream>
#include <climits>
#include <string>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

#include <cstdlib>

#define REMAIN 4
#define HALF_RESOURCE 12
#define TIME_OUT 1200

using namespace rapidjson;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace std;
using namespace alsched;

// policy_t::type policyType = policy_t::SJF_HETERO;
unsigned long milli_time();

class yarn_job_t {
    public:
        JobID jobId;
        job_t::type jobType;
        int32_t k;
        int32_t priority;
        double duration;
        double slowDuration;
        double come_time;

        yarn_job_t(const JobID jobId, const job_t::type jobType,
                   const int32_t k, const int32_t priority,
                   const double duration, const double slowDuration) {
            this->jobId = jobId;
            this->jobType = jobType;
            this->k = k;
            this->priority = priority;
            this->duration = duration;
            this->slowDuration = slowDuration;
            this->come_time = (double)milli_time() / 1000000.0;
        }
};

/* return timestamp in nanoseconds */
unsigned long milli_time() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec * 1000 * 1000 + time.tv_usec;
}

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{

private:
    /* use deque to simulate queue*/
    deque<yarn_job_t*> job_queue;
    bool * machine_alloc;
    int num_machines;
    int num_available;
    /* number of machines */
    int num_racks;
    /* machine number in each rack */
    int * num_rack_machine;
    /* machines in each rack */
    int ** racks;
    MachineType * rack_machine_type;
    /* lock */
    pthread_mutex_t lock;
    /* create pthread */
    bool created;
    long last_free_time;
    /* scheduling type */
    simtype_t::type simType;
    /* DEBUG */
    int cnt;

    void alloc_machine(int32_t machine) {
        machine_alloc[machine] = true;
    }

    void free_machine(int32_t machine) {
        machine_alloc[machine] = false;
    }
public:

    TetrischedServiceHandler()
    {
        if (pthread_mutex_init(&lock, NULL) != 0)
        {
            printf("\n mutex init failed\n");
            return;
        }
        printf("init\n");
        ReadConfigFile();
        machine_alloc = new bool[num_machines];
        memset(machine_alloc, 0, num_machines);
        srand((unsigned int) time(NULL));

        created = false;
        last_free_time = 0;

        cnt = 0;
    }

    /* read rack_cap from config-mini file */
    void ReadConfigFile() {

        /* get rack info */
        num_racks = 4;
        racks = new int*[num_racks];
        num_rack_machine = new int[num_racks];
        rack_machine_type = new MachineType[num_racks];
        int startMahineId = 0;
        for (SizeType i = 0; i < 4; i++) {
            // Uses SizeType instead of size_t
            int machineCount = (i == 0) ? 4 : 6;
            // get machine number in rack
            num_rack_machine[i] = machineCount;
            // store machine id in rack
            int * rack = new int[machineCount];
            for (int j = 0; j < machineCount; j++) {
                rack[j] = j + startMahineId;
            }
            // put rack into racks
            racks[i] = rack;
            // TODO: set type
            rack_machine_type[i] = machine_t::MACHINE_HDFS;
            startMahineId += machineCount;
        }
        rack_machine_type[0] = machine_t::MACHINE_GPU;
        num_machines = startMahineId;
        num_available = num_machines;

        simType = simtype_t::SOFT;
        printf("Scheduling policy is set to SOFT.\n");

    }

    bool ServeFirst() {
        yarn_job_t* job = job_queue.front();
        if(DispatchJob(job->jobId, job->jobType, job->k,
                       job->priority, job->duration, job->slowDuration)) {
            printf("- Serve first job\n");
            job_queue.pop_front();
            delete(job);
            return true;
        }
        return false;
    }

    bool ServeShortest() {
        /* initialize with a big enough number */
        double minDuration = (double) INT_MAX;
        /* initialize with NULL result */
        std::deque<yarn_job_t*>::iterator minJobIt = job_queue.end();
        /* loop queue to find shortest */
        std::deque<yarn_job_t*>::iterator it;
        for (it = job_queue.begin(); it != job_queue.end(); ++it) {
            yarn_job_t* job = *it;
            double duration = (double) INT_MAX;
            double current_time = (double)milli_time() / 1000000.0;
            // TODO: whether need to wait no enough resource job
            if (num_available < job -> k)
                continue;
            if (CanAllocPreferredResources(job -> jobType, job -> k)) {
                duration = job -> duration + current_time - job -> come_time;
            }
            else if (simType == simtype_t::SOFT) {
                duration = job -> slowDuration + current_time - job -> come_time;
            }
            if (duration < minDuration) {
                minDuration = duration;
                minJobIt = it;
            }
        }



        // printf("ServeShortest(): shortest duration %.3f.\n", minDuration);
        if (minJobIt != job_queue.end()) {
            if (minDuration > TIME_OUT && num_available < HALF_RESOURCE) return false;
            /* find one, schedule the job */
            yarn_job_t * minJob = *minJobIt;
            if(DispatchJob(minJob->jobId, minJob->jobType, minJob->k,
                           minJob->priority, minJob->duration, minJob->slowDuration)) {
                printf("- Serve shortest job %d, duration: %.3f, %d racks left\n", (int) minJob->jobId, minDuration, num_available);
                job_queue.erase(minJobIt);
                delete(minJob);
                return true;
            }
        }
        return false;
    }

    /* get first job from the front of queue and try to serve */
    bool ServeQueue() {
        if (job_queue.size() == 0 || num_available < 4)  // TODO: num_available < 4 HERE for debug
            return false;
        if (simType == simtype_t::NONE)
            return ServeFirst();
        return ServeShortest();
    }

    bool AllocResources(const JobID jobId, const set<int32_t> machines) {
        bool success = false;
        int yarnport = 9090;
        shared_ptr<TTransport> socket(new TSocket("localhost", yarnport));
        shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        YARNTetrischedServiceClient client(protocol);
        try {
            transport->open();
            client.AllocResources(jobId, machines);
            success = true;
            transport->close();
        }
        catch (TException& tx) {
            printf("ERROR calling YARN : %s\n", tx.what());
        }
        ShowMachines(machines);
        return success;
    }

    bool ScheduleStrictFCFS(std::set<int32_t> & machines, const int32_t k) {
        // FCFS + highest rank
        if (num_available < k)
            return false;
        int count = 0;
        for (int i = num_machines - 1; i >= 0; i--) {
            if (machine_alloc[i])
                continue;
            alloc_machine(i);
            machines.insert(i);
            count++;
            if (count >= k)
                break;
        }
        if (count < k) {
            FreeMachines(machines);
            return false;
        }
        return true;
    }

    // bool ScheduleSparseFCFS(std::set<int32_t> & machines, const int32_t k) {
    //     // FCFS + highest rank
    //     if (num_available < k)
    //         return false;
    //     int k_cp = k;
    //     for (int i = 1; i < num_racks; i++) {
    //         int vm_cnt = 0;
    //         /* if it is GPU machine rack, loop to find available */
    //         for (int j = 0; j < num_rack_machine[i]; j++) {
    //             int32_t machine = racks[i][j];
    //             if (!machine_alloc[machine]) {
    //                 vm_cnt++;
    //             }
    //         }
    //         for (int w = REMAIN; w < vm_cnt; w++) {
    //             if (k_cp <= 0) break;

    //             int j = 0;
    //             int32_t machine = racks[i][j];
    //             while (machine_alloc[machine]) j++;
    //             alloc_machine(machine);
    //             machines.insert(machine);
    //             k_cp--;
    //         }
    //         if (k_cp <= 0) return true;
    //     }
    //     return ScheduleStrictFCFS(machines, k_cp);
    // }

    bool ScheduleRandomFCFS(std::set<int32_t> & machines, const int32_t k) {
        if (num_available < k)
            return false;
        if (num_available == k) {
            /* alloc all remaining machines in this case
             * use this small optimization to avoid massive collisions */
            return ScheduleStrictFCFS(machines, k);
        }
        int randInt = rand() % num_machines;
        for (int i = 0; i < k; i++) {
            while (machine_alloc[randInt]) {
                randInt ++;
                randInt %= num_machines;
            }
            alloc_machine(randInt);
            machines.insert(randInt);
        }
        return true;
    }

    bool TryAllocGPUMachines(std::set<int32_t> & machines,
                             const job_t::type jobType, const int32_t k) {
        /* try to schedule on GPU machines */
        int count = 0;
        for (int i = 0; i < num_racks; i++) {
            if (rack_machine_type[i] == machine_t::MACHINE_GPU) {
                /* if it is GPU machine rack, loop to find available */
                for (int j = 0; j < num_rack_machine[i]; j++) {
                    int32_t machine = racks[i][j];
                    if (!machine_alloc[machine]) {
                        alloc_machine(machine);
                        machines.insert(machine);
                        count++;
                        if (count >= k) {
                            return true;
                        }
                    }
                }
            }
        }
        /* not enough, free pre-alloc machines */
        FreeMachines(machines);
        return false;
    }

    bool TryAllocSameRack(std::set<int32_t> & machines,
                          const job_t::type jobType, const int32_t k) {
        /* try to schedule on the same rack */
        int count = 0;
        // int bestnow = -1;
        // int bestnow_avai = 999;

        for (int i = num_racks - 1; i >= 0; i--) {
            int available = 0;
            /* acquire the number of available machines in the rack */
            for (int j = 0; j < num_rack_machine[i]; j++) {
                int32_t machine = racks[i][j];
                if (!machine_alloc[machine]) {
                    available++;
                }
            }
            if (available >= k) {
            // if (available > k) {
                /* enough machines, alloc on this rack */
                for (int j = 0; j < num_rack_machine[i]; j++) {
                    int32_t machine = racks[i][j];
                    if (!machine_alloc[machine]) {
                        alloc_machine(machine);
                        machines.insert(machine);
                        count++;
                        if (count >= k) {
                            return true;
                        }
                    }
                }
            }
            // else if ((available > k && available < bestnow_avai && i > 0) || (bestnow == -1 && i == 0)) {
            //     bestnow = i;
            //     bestnow_avai = available;
            // }
        }
        // FreeMachines(machines);
        return false;
    }

    /* try to schedule on preferred resources based on job type
     * return true and store machine id in machines if succeeded
     * otherwise, return false */
    bool TryAllocPreferredResources(std::set<int32_t> & machines,
                                         const job_t::type jobType, const int32_t k) {
        if (jobType == job_t::JOB_MPI) {
            return TryAllocSameRack(machines, jobType, k);
        }
        else {
            return TryAllocGPUMachines(machines, jobType, k);
        }
    }

    /* check if can schedule on preferred resources based on job type */
    bool CanAllocPreferredResources(const job_t::type jobType, const int32_t k) {
        set<int32_t> machines;
        if (TryAllocPreferredResources(machines, jobType, k)) {
            FreeMachines(machines);
            return true;
        }
        return false;
    }

    bool ScheduleHeteroFCFS(std::set<int32_t> & machines,
                            const job_t::type jobType, const int32_t k) {
        if (num_available < k)
            return false;
        if (!TryAllocPreferredResources(machines, jobType, k)) {
            if (simType == simtype_t::HARD) return false;
            // if (jobType == job_t::JOB_GPU) ScheduleSparseFCFS(machines, k);
            /* fail to schedule on preferred resources
             * free to schedule anywhere, use strict FCFS here */
            // else ScheduleStrictFCFS(machines, k);
            return ScheduleStrictFCFS(machines, k);
        }
        return true;
    }

    bool DispatchJob(const JobID jobId, const job_t::type jobType, const int32_t k,
                     const int32_t priority, const double duration, const double slowDuration) {
        // JOB_MPI prefers machines on one rack
        // JOB_GPU prefers big machines
        // TODO: race
        bool success = false;
        // try to allocate some nodes
        set<int32_t> machines;
        // switch on scheduling policy
        switch (simType) { 
            case simtype_t::NONE: 
                success = ScheduleRandomFCFS(machines, k);
                break;
            case simtype_t::SOFT:
                success = ScheduleHeteroFCFS(machines, jobType, k);
                break;
            case simtype_t::HARD:
                /* same as FCFS_HETERO
                 * difference in handling the queue */
                success = ScheduleHeteroFCFS(machines, jobType, k);
                break;
        }
        if (success) {
            printf("succeed in scheduling job %d\n", jobId);
            success = AllocResources(jobId, machines);
            if (success)
                num_available -= k;
            else {
                printf("NOT VERY SUCCESS...: ");
                num_available -= k;     // in case fail again
                ShowMachines(machines);
            }
        }
        return success;
    }

    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k,
                const int32_t priority, const double duration, const double slowDuration)
    {
        if (!created) {
            pthread_t tid;
            pthread_create(&tid, NULL, CheckServeQueue, this);
            created = true;
        }
        
        pthread_mutex_lock(&lock);
        printf("Comes Job %d: %f, %f; with %d racks demand.\n", jobId, duration, slowDuration, k);
        // Your implementation goes here
        if(simType != simtype_t::NONE || job_queue.size() != 0 ||
                !DispatchJob(jobId, jobType, k, priority, duration, slowDuration)) {
            /* no enough resources, add to queue */
            yarn_job_t* job = new yarn_job_t(jobId, jobType, k,
                                             priority, duration, slowDuration);
            job_queue.push_back(job);
            printf("Add job %d to queue, %d jobs in the queue, %d available racks.\n", jobId, (int)job_queue.size(), num_available);
        }
        else
            printf("- Serve the Job %d, %d racks left\n", jobId, num_available);
        pthread_mutex_unlock(&lock);
    }

    void FreeMachines(const std::set<int32_t> & machines) {
        // Free up resources
        std::set<int32_t>::iterator it;
        for (it = machines.begin(); it != machines.end(); ++it) {
            free_machine(*it);
        }
    }

    void ShowMachines(const std::set<int32_t> & machines) {
        // Free up resources
        std::set<int32_t>::iterator it;
        printf("[Show Machines] racks: ");
        for (it = machines.begin(); it != machines.end(); ++it) {
            int r = (*it + 2) / 6 + 1;
            int h = (r == 1) ? 0 : 2;
            h += *it - (r-1) * 6;
            printf(" r%dh%d, ", r, h);
        }
        printf("\n");
    }

    void ShowFreeMachines() {
        // show free resources
        printf("[Show Free Machines] racks: ");
        for (int i = num_machines - 1; i >= 0; i--) {
            if (!machine_alloc[i]) {
                int r = (i + 2) / 6 + 1;
                int h = (r == 1) ? 0 : 2;
                h += i - (r-1) * 6;
                printf(" r%dh%d, ", r, h);
            }
        }
        printf("\n");
    }

    void FreeResources(const std::set<int32_t> & machines)
    {
        pthread_mutex_lock(&lock);
        // Your implementation goes here
        printf("FreeResources\n");
        // free machines
        FreeMachines(machines);
        last_free_time = milli_time();
        // assume size is correct
        num_available += machines.size();
        pthread_mutex_unlock(&lock);
    }

    static void *CheckServeQueue(void * args) {
        /* keep serve until no enough resources */
        TetrischedServiceHandler * obj = (TetrischedServiceHandler *) args;
        while(1) {
            sleep(2);
            if(milli_time() - obj -> last_free_time < 100 * 1000)
                sleep(1);
            pthread_mutex_lock(&(obj->lock));
            while(obj -> ServeQueue());
            pthread_mutex_unlock(&(obj->lock));
            obj -> cnt ++;
            if (obj -> cnt % 100 == 0) {
                printf("[CheckServeQueue] %d jobs in the queue with %d num_available.\n", (int) obj -> job_queue.size(), obj->num_available);
                obj -> ShowFreeMachines();
            }
        }
        return NULL;
    }
};

int main(int argc, char **argv)
{

    //create a listening server socket
    int alschedport = 9091;
    shared_ptr<TetrischedServiceHandler> handler(new TetrischedServiceHandler());
    shared_ptr<TProcessor> processor(new TetrischedServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(alschedport));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
