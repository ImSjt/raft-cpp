#ifndef __CRAFT_STATUS_H__
#define __CRAFT_STATUS_H__

#include <string>
#include <algorithm>

namespace craft {

class Status {
 public:
    // Create a success status.
    Status() : state_(nullptr) {}
    ~Status() { delete[] state_; }

    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    Status(Status&& rhs) : state_(rhs.state_) { rhs.state_ = nullptr; }
    Status& operator=(Status&& rhs);

    static Status OK() { return Status{}; }

    static Status Error(const char* format, ...);

    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == nullptr); }

    const char* Str() const { return state_ == nullptr ? "OK" : state_; }

 private:
    Status(char* state) : state_(state) {}
    static char* CopyState(char* dst, const char* src);

 private:
    char* state_;
    static const int32_t kStateMaxSize = 1024;
};

inline Status::Status(const Status& rhs)
    : state_(nullptr) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(state_, rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
    // The following condition catches both aliasing (when this == &rhs),
    // and the common case where both rhs and *this are ok.
    if (state_ != rhs.state_) {
        if (rhs.state_ == nullptr) {
            delete[] state_;
            state_ = nullptr;
        } else {
            state_ = CopyState(state_, rhs.state_);
        }
    }
    return *this;
}

inline Status& Status::operator=(Status&& rhs) {
    std::swap(state_, rhs.state_);
    return *this;
}

} // namespace craft

#endif // __CRAFT_STATUS_H__