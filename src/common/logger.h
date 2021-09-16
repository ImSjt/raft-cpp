#ifndef __CRAFT_COMMON_LOGGER_H__
#define __CRAFT_COMMON_LOGGER_H__
#include <cstdint>
#include <functional>

#include "common/singleton.h"
#include "common/fixed_buffer.h"

namespace craft {

// logger singleton
class Logger : public Singleton<Logger> {
 public:
    // log level
    enum LogLevel {
        kTrace,
        kDebug,
        kInfo,
        kWarn,
        kError,
        kFatal,
        kNumLogLevel
    };

    // temporary buffer
    using Buffer = FixedBuffer<kSmallBuffer>;

    // log output callback
    using LogOutputCb = std::function<void (const char* str, int32_t len, bool fatal)>;
    
    Logger();

    void RegisterCallback(LogOutputCb log_output);

    void SetLevel(LogLevel level) {
        level_ = level;
    }

    LogLevel GetLevel() {
        return level_;
    }

    void Write(LogLevel level, const char* file, int32_t line, const char* format, ...);

 private:
    LogOutputCb log_output_;
    LogLevel level_;
};

#define LOG_TRACE(format, ...) \
    if (Logger::kTrace >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kTrace, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_DEBUG(format, ...) \
    if (Logger::kDebug >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kDebug, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_INFO(format, ...) \
    if (Logger::kInfo >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kInfo, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_WARNING(format, ...) \
    if (Logger::kWarn >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kWarn, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_ERROR(format, ...) \
    if (Logger::kError >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kError, __FILE__, __LINE__, format, ##__VA_ARGS__)

#define LOG_FATAL(format, ...) \
    if (Logger::kFatal >= Logger::Instance().GetLevel()) \
        Logger::Instance().Write(Logger::kFatal, __FILE__, __LINE__, format, ##__VA_ARGS__)

}; // namespace craft

#endif // __CRAFT_COMMON_LOGGER_H__