#include <iostream>

#include <fstream>
#include <sstream>
#include <vector>
#include <string>

#include <pthread.h>

#include <chrono>

#include <thread>

class SimulationClock {
private: 
    std::chrono::steady_clock::time_point realStart;
    std::chrono::seconds simOffset; // 3600 (10 hrs)
    double speedup; // 60.0 means 1 real seconds = 60 sim secs

public:
    SimulationClock(std::chrono::seconds simOffset, double speedup) {
        this->realStart = std::chrono::steady_clock::now();
        this->simOffset = simOffset;
        this->speedup = speedup;
    }

    std::chrono::seconds simNow() {
        auto realElapsed = std::chrono::steady_clock::now() - realStart;
        auto simElapsed = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::duration<double>(realElapsed) * speedup);
        return simOffset + simElapsed;
    }

    void sleep_until_sim(std::chrono::seconds target) {
        std::chrono::seconds cur = simNow();
        if (target <= cur) return;
        // compute required real sleep using inverse of speed
        auto sim_wait = target - cur;
        auto real_wait = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double>(sim_wait.count() / speedup)
        );
        std::this_thread::sleep_for(real_wait);
    }
};

struct TrafficLightData {
std::chrono::seconds seconds;
int trafficLightID;
int carsPassed;
};

static inline bool parse_hms_to_seconds(const std::string& s, std::chrono::seconds& out) {
int H=0, M=0, S=0; char c1=':', c2=':';
std::istringstream is(s);
if (!(is >> H >> c1 >> M >> c2 >> S) || c1 != ':' || c2 != ':') return false;
if (H < 0 || H > 23 || M < 0 || M > 59 || S < 0 || S > 59) return false;
out = std::chrono::hours(H) + std::chrono::minutes(M) + std::chrono::seconds(S);
return true;
}

std::vector<TrafficLightData> readFromCSV(std::string fileName) {
/*
Bring data from disk to memory, to simulate parallelisation without bottleneck from I/O.
*/

std::ifstream file(fileName);
std::vector<TrafficLightData> allData;

std::string line;
std::getline(file ,line); // read (skip) header line

while (std::getline(file, line)) { // line = "10:00:00,1,3"
    std::stringstream ss(line);

    std::string temp; // used to hold a part of the line
    std::chrono::seconds seconds{0}; // time stamp should be created now
    int trafficLightID;
    int carsPassed;

    std::getline(ss, temp, ','); // "10:00:00"
    parse_hms_to_seconds(temp, seconds); // 36000

    std::getline(ss, temp, ','); // "1"
    trafficLightID = std::stoi(temp); // 1

    std::getline(ss, temp, ','); // "3"
    carsPassed = std::stoi(temp); // 3

    TrafficLightData data {
        seconds,
        trafficLightID,
        carsPassed
    };

    allData.push_back(data);
} 

return allData;
}

class Buffer {
private:
    std::queue<TrafficLightData> queue;
    size_t maxSize;
    std::mutex queueLock;
    std::condition_variable notFull, notEmpty;

public:
    Buffer(size_t maxSize) {
        this->maxSize = maxSize;
    }

    void enqueue(const TrafficLightData& entry) {
        std::unique_lock<std::mutex> lock(queueLock);
        notFull.wait(lock, [this]() {return queue.size() < maxSize; });
        queue.push(entry);
        notEmpty.notify_one();
    }

    TrafficLightData dequeue() {
        std::unique_lock<std::mutex> lock(queueLock);
        notEmpty.wait(lock, [this]() {return !queue.empty();});
        TrafficLightData entry = queue.front();
        queue.pop();
        notFull.notify_one();
        return entry;
    }
};

class SortedTrafficList {
private:
    int topN;

    // hours are 0 - 23
    std::unordered_map<int, std::unordered_map<int, int>> counts; // hour -> <id -> count>
    std::mutex listLock;

public:
    SortedTrafficList(int n) {
        this->topN = n;
    }

    void add(TrafficLightData entry) {
        int hour = std::chrono::duration_cast<std::chrono::hours>(entry.seconds).count() % 24;
        {
            std::lock_guard<std::mutex> lock(listLock);
            counts[hour][entry.trafficLightID] += entry.carsPassed;
        }
        // std::cout << "Added: (Hour, ID, count) " << hour << " " << entry.trafficLightID << " " << entry.carsPassed << std::endl;
        // printState();
        std::this_thread::sleep_for(std::chrono::seconds(2)); // simulate work needed to consume
    }

    void printState() {
        std::cout << "Current counts:\n";
        for (const auto &hourPair : counts) {
            int hour = hourPair.first;
            for (const auto &idPair : hourPair.second) {
                int id = idPair.first;
                int count = idPair.second;
                std::cout << "(Hour=" << hour
                        << ", ID=" << id
                        << ", Count=" << count << ")\n";
            }
        }
    }


};

struct ProducerArgs {
std::vector<TrafficLightData>* data;
int assignedID;
Buffer* buffer;
SimulationClock* clock;
};

void* ProducerTask(void* threadArgs) {
auto* args = static_cast<ProducerArgs*>(threadArgs);

for (auto& entry : *(args->data)) {
    if (entry.trafficLightID != args->assignedID) continue;

    // only enqueue if timestamp matches simulation time :)
    args->clock->sleep_until_sim(entry.seconds);

    args->buffer->enqueue(entry);
}
return nullptr;
}

struct ConsumerArgs {
Buffer* buffer;
SortedTrafficList* list;
int n;
};

void* ConsumerTask(void* threadArgs) {
auto* args = static_cast<ConsumerArgs*>(threadArgs);
TrafficLightData entry; // process an entry at a time

while (true) {
    entry = args->buffer->dequeue();
    args->list->add(entry);
}
};


int main() { // act as timer thread
std::string fileName = "traffic_6h_random.csv";
std::vector<TrafficLightData> data = readFromCSV(fileName);
Buffer buffer {1};
SortedTrafficList list{3}; // shows top 3
SimulationClock clock{std::chrono::seconds {10 * 3600}, 60.0}; // start at 10am and run at 60x

int numProducers = 1;
int numConsumers = 1;

std::vector<pthread_t> producers(numProducers);
std::vector<ProducerArgs> producerArgs(numProducers);

std::vector<pthread_t> consumers(numConsumers);
std::vector<ConsumerArgs> consumerArgs(numConsumers);

for (int i = 0; i < numProducers; i++) {
    producerArgs[i] = {&data, i + 1, &buffer, &clock};
    pthread_create(&producers[i], nullptr, ProducerTask, &producerArgs[i]);
}

for (int i = 0; i < numConsumers; i++) {
    consumerArgs[i] = {&buffer, &list, 3};
    pthread_create(&consumers[i], nullptr, ConsumerTask, &consumerArgs[i]);
}

std::this_thread::sleep_for(std::chrono::seconds(60)); // simulate work needed to consume

return 0;
}