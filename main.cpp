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

    void simSleepUntil(std::chrono::seconds target) {
        std::chrono::seconds cur = simNow();
        if (target <= cur) return;
        // compute required real sleep using inverse of speed
        auto sim_wait = target - cur;
        auto real_wait = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double>(sim_wait.count() / speedup)
        );
        std::this_thread::sleep_for(real_wait);
    }

    int simGetCurHour() {
        int now = std::chrono::duration_cast<std::chrono::hours>(simNow()).count() % 24;
        return now;
    }

    int simGetHourFromSecs(std::chrono::seconds seconds) {
        int hour = std::chrono::duration_cast<std::chrono::hours>(seconds).count() % 24;
        return hour;
    }
};

struct TrafficLightData {
    bool signalReport;
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
            false,
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
            // printAll();
            notEmpty.notify_one();
        }

        void printAll() {
            std::unique_lock<std::mutex> lock(queueLock);
            std::queue<TrafficLightData> copy = queue;  // make a copy so we don't disturb order

            std::cout << "Buffer contents (" << copy.size() << " items):\n";
            while (!copy.empty()) {
                const auto &e = copy.front();
                int h = std::chrono::duration_cast<std::chrono::hours>(e.seconds).count() % 24;
                std::cout << "Hour=" << h
                        << " ID=" << e.trafficLightID
                        << " Cars=" << e.carsPassed
                        << "\n";
                copy.pop();
            }
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
        std::unordered_map<int, int> idCounts; // <id -> count>
        std::mutex listLock;

    public:
        SortedTrafficList(int n) {
            this->topN = n;
        }

        void add(TrafficLightData entry) {
            std::this_thread::sleep_for(std::chrono::seconds(2)); // simulate work needed to consume
            {
                std::lock_guard<std::mutex> lock(listLock);
                idCounts[entry.trafficLightID] += entry.carsPassed;
            }
            std::this_thread::sleep_for(std::chrono::seconds(3)); // simulate work needed to consume
        }

        void report() {
            std::lock_guard<std::mutex> lock(listLock);

            // Copy data into vector
            std::vector<std::pair<int,int>> counts(idCounts.begin(), idCounts.end());

            // Sort descending by count
            std::sort(counts.begin(), counts.end(),
                    [](auto &a, auto &b) {
                        return a.second > b.second;
                    });

            // Print topN or all
            int limit = std::min(topN, (int)counts.size());
            for (int i = 0; i < limit; i++) {
                std::cout << "(ID=" << counts[i].first
                        << ", Count=" << counts[i].second << ")\n";
            }

            clear();
        }

        void clear() {
            idCounts = {};
        }
};

struct ProducerArgs {
    std::vector<TrafficLightData>* data;
    std::vector<int> assignedIDs;
    Buffer* buffer;
    SimulationClock* clock;
};

void* ProducerTask(void* threadArgs) {
    auto* args = static_cast<ProducerArgs*>(threadArgs);

    for (auto& entry : *(args->data)) {

        // skip if traffic light id on in producer's list
        if (std::find(
            args->assignedIDs.begin(), 
            args->assignedIDs.end(), 
            entry.trafficLightID) == args -> assignedIDs.end()) {
                continue;
            }
        
        // only enqueue if timestamp matches simulation time :)
        args->clock->simSleepUntil(entry.seconds);

        args->buffer->enqueue(entry);

        std::cout << "Enqueue: " << entry.trafficLightID << std::endl;
    }
    return nullptr;
}

struct ConsumerArgs {
    Buffer* buffer;
    SortedTrafficList* list;
    int n;
    SimulationClock* clock;
};

void* ConsumerTask(void* threadArgs) {
    auto* args = static_cast<ConsumerArgs*>(threadArgs);
    TrafficLightData entry; // process an entry at a time

    while (true) {
        entry = args->buffer->dequeue();

        if (entry.signalReport == true) {
            args->list->report();
            continue; // no need to add it
        }

        args->list->add(entry);
    }
};


int main() { // act as timer thread
    std::string fileName = "output.csv";
    std::vector<TrafficLightData> data = readFromCSV(fileName);
    Buffer buffer {1};
    SortedTrafficList list{3}; // shows top 3
    SimulationClock clock{std::chrono::seconds {10 * 3600}, 60.0}; // start at 10am and run at 60x

    int numProducers = 3;
    int numTrafficLights = 4;
    int numConsumers = 2;

    // calculate how many Traffic Light ID's each producer should watch
    int base = numTrafficLights / numProducers;
    int remainder = numTrafficLights % numProducers;

    std::vector<pthread_t> producers(numProducers);
    std::vector<ProducerArgs> producerArgs(numProducers);

    std::vector<pthread_t> consumers(numConsumers);
    std::vector<ConsumerArgs> consumerArgs(numConsumers);

    int start = 0;
    for (int i = 0; i < numProducers; i++) {
        int numIDs = base + (i < remainder ? 1 : 0);

        std::vector<int> ids; 
        ids.reserve(numIDs);

        for (int j = 0; j < numIDs; j++) {
            ids.push_back(start + j); // 0 based ID's
        }

        start += numIDs;

        producerArgs[i] = {&data, ids, &buffer, &clock};
        pthread_create(&producers[i], nullptr, ProducerTask, &producerArgs[i]);
        pthread_detach(producers[i]);
    }

    for (int i = 0; i < numConsumers; i++) {
        consumerArgs[i] = {&buffer, &list, 3, &clock};
        pthread_create(&consumers[i], nullptr, ConsumerTask, &consumerArgs[i]);
        pthread_detach(consumers[i]);
    }

    std::cout << "Program Starting" << std::endl;

    while(true) {
        std::this_thread::sleep_for(std::chrono::seconds(60));

        TrafficLightData signalReport {
            true,
            std::chrono::seconds {0},
            -1, // arbitrary traffic light ID
            0 // adding 0 will not affect report
        };

        std::cout << "Enqueued Report Signal" << std::endl;

        buffer.enqueue(signalReport);
    }

    return 0;
}