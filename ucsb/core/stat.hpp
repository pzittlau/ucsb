#pragma once

#include <sys/times.h>
#include <unistd.h>
#include <string>
#include <fstream>
#include <limits>
#include <chrono>
#include <thread>

namespace ucsb {

struct cpu_stat_t {

    inline cpu_stat_t(size_t request_delay = 300)
        : last_cpu_(0), last_user_(0), last_sys_(0), request_delay_(request_delay), requests_count_(0),
          time_to_die_(true) {}
    inline ~cpu_stat_t() { stop(); }

    struct stat_t {
        double min = std::numeric_limits<double>::max();
        double max = 0;
        double avg = 0;
    };

    inline void start() {
        if (!time_to_die_)
            return;

        percent_.min = std::numeric_limits<double>::max();
        percent_.max = 0;
        percent_.avg = 0;

        requests_count_ = 0;
        time_to_die_ = false;
        thread_ = std::thread(&cpu_stat_t::request_cpu_usage, this);
    }
    inline void stop() {
        if (time_to_die_)
            return;

        time_to_die_ = true;
        thread_.join();
        last_cpu_ = 0;
        last_user_ = 0;
        last_sys_ = 0;
    }

    inline stat_t percent() const { return percent_; }

  private:
    void recalculate(double percent) {
        percent_.min = std::min(percent, percent_.min);
        percent_.max = std::max(percent, percent_.max);
        percent_.avg = (percent_.avg * (requests_count_ - 1) + percent) / requests_count_;
    }

    void request_cpu_usage() {
        size_t processor_count = std::thread::hardware_concurrency();
        tms time_sample;
        last_cpu_ = times(&time_sample);
        last_user_ = time_sample.tms_utime;
        last_sys_ = time_sample.tms_stime;

        while (!time_to_die_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_));

            clock_t cpu = times(&time_sample) * processor_count;
            clock_t user = time_sample.tms_utime;
            clock_t sys = time_sample.tms_stime;
            clock_t delta_proc = (user - last_user_) + (sys - last_sys_);
            clock_t delta_time = cpu - last_cpu_;

            double overall_cpus_percent = 100.0 * delta_proc / delta_time;
            double single_cpu_percent = overall_cpus_percent * processor_count;

            last_cpu_ = cpu;
            last_user_ = user;
            last_sys_ = sys;

            ++requests_count_;
            recalculate(single_cpu_percent);
        }
    }

    stat_t percent_;
    clock_t last_cpu_;
    clock_t last_user_;
    clock_t last_sys_;

    size_t request_delay_;
    size_t requests_count_;
    std::thread thread_;
    bool time_to_die_;
};

struct mem_stat_t {

    inline mem_stat_t(size_t request_delay = 100)
        : request_delay_(request_delay), requests_count_(0), time_to_die_(true) {}
    inline ~mem_stat_t() { stop(); }

    struct stat_t {
        size_t min = std::numeric_limits<size_t>::max();
        size_t max = 0;
        size_t avg = 0;
    };

    inline void start() {
        if (!time_to_die_)
            return;

        vm_.min = std::numeric_limits<size_t>::max();
        vm_.max = 0;
        vm_.avg = 0;

        rss_.min = std::numeric_limits<size_t>::max();
        rss_.max = 0;
        rss_.avg = 0;

        requests_count_ = 0;
        time_to_die_ = false;
        thread_ = std::thread(&mem_stat_t::request_mem_usage, this);
    }
    inline void stop() {
        if (time_to_die_)
            return;

        time_to_die_ = true;
        thread_.join();
    }

    inline stat_t vm() const { return vm_; }
    inline stat_t rss() const { return rss_; }

  private:
    void recalculate(size_t vm, size_t rss) {
        vm_.min = std::min(vm, vm_.min);
        vm_.max = std::max(vm, vm_.max);
        vm_.avg = (vm_.avg * (requests_count_ - 1) + vm) / requests_count_;

        rss_.min = std::min(rss, rss_.min);
        rss_.max = std::max(rss, rss_.max);
        rss_.avg = (rss_.avg * (requests_count_ - 1) + rss) / requests_count_;
    }

    void request_mem_usage() {
        while (!time_to_die_) {
            size_t vm = 0, rss = 0;
            mem_usage(vm, rss);
            ++requests_count_;
            recalculate(vm, rss);
            std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_));
        }
    }

    void mem_usage(size_t& vm, size_t& rss) {
        vm = 0;
        rss = 0;

        std::ifstream stat("/proc/self/stat", std::ios_base::in);
        std::string pid, comm, state, ppid, pgrp, session, tty_nr;
        std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
        std::string utime, stime, cutime, cstime, priority, nice;
        std::string O, itrealvalue, starttime;
        stat >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >> minflt >> cminflt >>
            majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
            starttime >> vm >> rss;
        stat.close();

        size_t page_size = sysconf(_SC_PAGE_SIZE);
        rss = rss * page_size;
    }

    stat_t vm_;
    stat_t rss_;

    size_t request_delay_;
    size_t requests_count_;
    std::thread thread_;
    bool time_to_die_;
};

} // namespace ucsb