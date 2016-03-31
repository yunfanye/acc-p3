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
#include <string>

#include <cstdlib>

using namespace rapidjson;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace std;
using namespace alsched;

class yarn_job_t {
    public:
        JobID jobId;
        job_t::type jobType;
        int32_t k;
        int32_t priority;
        double duration;
        double slowDuration;

        yarn_job_t(const JobID jobId, const job_t::type jobType,
                   const int32_t k, const int32_t priority,
                   const double duration, const double slowDuration) {
            this->jobId = jobId;
            this->jobType = jobType;
            this->k = k;
            this->priority = priority;
            this->duration = duration;
            this->slowDuration = slowDuration;
        }
};

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{

private:
    queue<yarn_job_t*> job_queue;
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

    void alloc_machine(int32_t machine) {
        machine_alloc[machine] = true;
    }

    void free_machine(int32_t machine) {
        machine_alloc[machine] = false;
    }
public:

    TetrischedServiceHandler()
    {
        // Your initialization goes here
        printf("init\n");
        ReadConfigFile();
        machine_alloc = new bool[num_machines];
        memset(machine_alloc, 0, num_machines);
        srand(NULL);
    }

    /* read rack_cap from config-mini file */
    void ReadConfigFile() {
        const char * inFileName = "config-mini";
        ifstream inFile;
        inFile.open(inFileName);//open the input file
        stringstream strStream;
        strStream << inFile.rdbuf();//read the file
        string jsonStr = strStream.str();//str holds the content of the file
        const char * json = jsonStr.c_str();
        Document document;
        document.Parse(json);
        const Value& rackCap = document["rack_cap"];
        /* get rack info */
        num_racks = rackCap.Size();
        racks = new int*[num_racks];
        num_rack_machine = new int[num_racks];
        rack_machine_type = new MachineType[num_racks];
        int startMahineId = 0;
        for (SizeType i = 0; i < num_racks; i++) {
            // Uses SizeType instead of size_t
            int machineCount = rackCap[i].GetInt();
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
    }

    /* get first job from the front of queue and try to serve */
    void ServeQueue() {
        if (job_queue.size() == 0)
            return;
        yarn_job_t* job = job_queue.front();
        if(DispatchJob(job->jobId, job->jobType, job->k,
                       job->priority, job->duration, job->slowDuration)){
            job_queue.pop();
            delete(job);
        }
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
        return success;
    }

    bool ScheduleStrictFCFS(std::set<int32_t> & machines, const int32_t k) {
        // FCFS + highest rank
        if (num_available < k)
            return false;
        int count = 0;
        for (int i = 0; i < num_machines; i++) {
            if (machine_alloc[i])
                continue;
            alloc_machine(i);
            machines.insert(i);
            count++;
            if (count >= k)
                break;
        }
        return true;
    }

    bool ScheduleRandomFCFS(std::set<int32_t> & machines, const int32_t k) {
        if (num_available < k)
            return false;
        for (int i = 0; i < k; i++) {
            int randInt = rand() % num_machines;
            while (machine_alloc[randInt]) {
                randInt = rand() % num_machines;
            }
            alloc_machine(i);
            machines.insert(i);
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
        for (int i = 0; i < num_racks; i++) {
            int available = 0;
            /* acquire the number of available machines in the rack */
            for (int j = 0; j < num_rack_machine[i]; j++) {
                int32_t machine = racks[i][j];
                if (!machine_alloc[machine]) {
                    available++;
                }
            }
            if (available >= k) {
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
        }
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

    bool ScheduleHeteroFCFS(std::set<int32_t> & machines,
                            const job_t::type jobType, const int32_t k) {
        if (num_available < k)
            return false;
        if (!TryAllocPreferredResources(machines, jobType, k)) {
            /* fail to schedule on preferred resources
             * free to schedule anywhere, use strict FCFS here */
            ScheduleStrictFCFS(machines, k);
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
        success = ScheduleStrictFCFS(machines, k);
        success = ScheduleRandomFCFS(machines, k);
        success = ScheduleHeteroFCFS(machines, jobType, k);
        if (success) {
            printf("succeed in scheduling job %d\n", jobId);
            success = AllocResources(jobId, machines);
            num_available -= k;
        }
        return success;
    }

    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k,
                const int32_t priority, const double duration, const double slowDuration)
    {
        // Your implementation goes here
        printf("AddJob\n");
        if(job_queue.size() != 0 ||
                !DispatchJob(jobId, jobType, k, priority, duration, slowDuration)) {
            printf("add job %d to queue\n", jobId);
            yarn_job_t* job = new yarn_job_t(jobId, jobType, k,
                                             priority, duration, slowDuration);
            job_queue.push(job);
        }
    }

    void FreeMachines(const std::set<int32_t> & machines) {
        // Free up resources
        std::set<int32_t>::iterator it;
        for (it = machines.begin(); it != machines.end(); ++it) {
            free_machine(*it);
        }
    }

    void FreeResources(const std::set<int32_t> & machines)
    {
        // Your implementation goes here
        printf("FreeResources\n");
        // free machines
        FreeMachines(machines);
        // assume size is correct
        num_available += machines.size();
        ServeQueue();
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
