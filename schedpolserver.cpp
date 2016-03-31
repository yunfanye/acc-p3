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

using namespace rapidjson;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace std;
using namespace alsched;

class yarn_job_t
{
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
        int count = 0;
        for (SizeType i = 0; i < rackCap.Size(); i++) // Uses SizeType instead of size_t
            count += rackCap[i].GetInt();
        num_machines = count;
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

    bool DispatchJob(const JobID jobId, const job_t::type jobType, const int32_t k,
                     const int32_t priority, const double duration, const double slowDuration) {
        // JOB_MPI prefers machines on one rack
        // JOB_GPU prefers big machines
        // TODO: race
        bool success = false;
        // try to allocate some nodes
        set<int32_t> machines;
        // FCFS + highest rank
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
        if(count < k) {
            // free pre-alloc resources
            std::set<int32_t>::iterator it;
            for (it = machines.begin(); it != machines.end(); ++it) {
                free_machine(*it);
            }
            // no enough resources, return false
        }
        else {
            printf("succeed in scheduling job %d\n", jobId);
            success = AllocResources(jobId, machines);
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

    void FreeResources(const std::set<int32_t> & machines)
    {
        // Your implementation goes here
        printf("FreeResources\n");
        // Free up resources
        std::set<int32_t>::iterator it;
        for (it = machines.begin(); it != machines.end(); ++it) {
            free_machine(*it);
        }
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
