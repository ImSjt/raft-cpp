#include "craft/status.h"

#include <cstdio>
#include <cstdarg>
#include <cstring>

namespace craft {

char* Status::CopyState(char* dst, const char* src) {
    if (src == nullptr) {
        return dst;
    }

    if (dst == nullptr) {
        dst = new char[kStateMaxSize];
    }

    std::memcpy(dst, src, kStateMaxSize);

    return dst;
}

Status Status::Error(const char* format, ...) {
    char* state = new char[kStateMaxSize];
    va_list vlist;

    va_start(vlist, format);
    vsnprintf(state, kStateMaxSize, format, vlist);
    va_end(vlist);

    return Status(state);
}

} // namespace craft