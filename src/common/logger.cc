#include "common/logger.h"

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstdarg>
#include <ctime>
#include <iostream>
#include <map>
#include <string>
#include <chrono>

namespace craft {

static const char* g_kLogLevel[Logger::kNumLogLevel] = {
    "TRACE",
    "DEBUG",
    "INFO ",
    "WARN ",
    "ERROR",
    "FATAL"
};

static void DefaultLogOutput(const char* str, bool flush);

Logger::Logger()
    :log_output_(DefaultLogOutput),
     level_(kTrace) {

}

void Logger::RegisterCallback(LogOutputCb log_output) {
    assert(log_output != nullptr);
    
    log_output_ = log_output;
}

// 20210318 17:36:52 [DEBUG]: hello world [test.cc:7]
void Logger::Write(LogLevel level, const char* file, int32_t line, const char* format, ...) {
    assert(log_output_ != nullptr);
    assert(level >= kTrace && level <= kFatal);

    // 4k buffer on stack
    Buffer buffer;
    va_list vlist;

    // format time
    auto tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    struct tm* t = localtime(&tt);
    int32_t len = snprintf(buffer.Current(), buffer.Avail(), "%4d%02d%02d %02d:%02d:%02d",
                        t->tm_year + 1900, t->tm_mon + 1, t->tm_mday,
                            t->tm_hour, t->tm_min, t->tm_sec);
    buffer.Add(len);

    // log level
    len = snprintf(buffer.Current(), buffer.Avail(), " [%s]: ", g_kLogLevel[level]);
    buffer.Add(len);

    // log info
    va_start(vlist, format);
    len = vsnprintf(buffer.Current(), buffer.Avail(), format, vlist);
    va_end(vlist);
    buffer.Add(len);

    // file and line
    len = snprintf(buffer.Current(), buffer.Avail(), " [%s:%d]\n", file, line);
    buffer.Add(len);

    log_output_(buffer.Data(), true);

    if (level == kFatal) {
        abort();
    }
}

static void DefaultLogOutput(const char* str, bool flush) {
    std::cout << str;
}

} // namespace craft