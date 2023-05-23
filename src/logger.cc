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
#include "src/logger.h"

#include <cstdio>
#include <cstdlib>

namespace craft {

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_RESET   "\x1b[0m"

void Logger::Log(LogLevel level, const char* format, ...) {
  if (level < GetLogLevel()) {
    return;
  }

  va_list ap;
  va_start(ap, format);
  Logv(level, format, ap);
  va_end(ap);
}

void ConsoleLogger::Logv(LogLevel level, const char* format, va_list ap) {
  std::lock_guard lg(lock_);
  printf(GetPrefix(level));
  vprintf(format, ap);
  printf("\n");
  printf(ANSI_COLOR_RESET);

  if (level == LogLevel::kFatal) {
    abort();
  }
}

const char* ConsoleLogger::GetPrefix(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return ANSI_COLOR_GREEN "[DEBUG] ";
    case LogLevel::kInfo:
      return ANSI_COLOR_YELLOW "[INFO] ";
    case LogLevel::kWarning:
      return ANSI_COLOR_MAGENTA "[WARNING] ";
    case LogLevel::kError:
      return ANSI_COLOR_RED "[ERROR] ";
    case LogLevel::kFatal:
      return ANSI_COLOR_RED "[FATAL] ";
    default:
      return "";
  }
}

};