// Copyright 2023 JT
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <cstdint>
#include <cstdarg>
#include <mutex>

namespace craft {

enum LogLevel : uint8_t {
  kDebug,
  kInfo,
  kWarning,
  kError,
  kFatal,
};

class Logger {
 public:
  Logger(LogLevel log_level) : log_level_(log_level) {}
  virtual ~Logger() = default;

  virtual void Log(LogLevel, const char* format, ...);
  virtual void Logv(LogLevel, const char* format, va_list ap) = 0;

  LogLevel GetLogLevel() const { return log_level_; }
  void SetLogLevel(LogLevel level) { log_level_ = level; }

 private:
  LogLevel log_level_;
};

class ConsoleLogger : public Logger {
 public:
  ConsoleLogger(LogLevel log_level = LogLevel::kDebug)
    : Logger(log_level) {}

  void Logv(LogLevel level, const char* format, va_list ap) override;

 private:
  const char* GetPrefix(LogLevel level);

  std::mutex lock_;
};

#define CRAFT_LOG_DEBUG(logger, format, ...) \
  logger->Log(::craft::LogLevel::kDebug, format, ##__VA_ARGS__)

#define CRAFT_LOG_INFO(logger, format, ...) \
  logger->Log(::craft::LogLevel::kInfo, format, ##__VA_ARGS__)

#define CRAFT_LOG_WARNING(logger, format, ...) \
  logger->Log(::craft::LogLevel::kWarning, format, ##__VA_ARGS__)

#define CRAFT_LOG_ERROR(logger, format, ...) \
  logger->Log(::craft::LogLevel::kError, format, ##__VA_ARGS__)

#define CRAFT_LOG_FATAL(logger, format, ...) \
  logger->Log(::craft::LogLevel::kFatal, format, ##__VA_ARGS__)

}  // namespace craft