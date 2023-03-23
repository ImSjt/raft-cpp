#ifndef __CRAFT_COMMON_FIXED_BUFFER_H__
#define __CRAFT_COMMON_FIXED_BUFFER_H__

#include <cstdint>
#include <cstring>

namespace craft {

const int kSmallBuffer = 4096;
const int kLargeBuffer = 4096*1024;

template<int32_t size>
class FixedBuffer {
 public:
    FixedBuffer(int32_t index = -1) :
        index_(index),
        cur_(data_) { }

    void Append(const char* buf, int32_t len) {
        if (Avail() >= len) {
            memcpy(cur_, buf, len);
            Add(len);
        }
    }

    const char* Data() const { return data_; }
    int32_t Length() const { return static_cast<int32_t>(cur_ - data_); }

    char* Current() { return cur_; }
    int32_t Avail() const { return static_cast<int32_t>(End() - cur_); }
    void Add(int32_t len) { cur_ += len; }

    void Reset() { cur_ = data_; }
    void BZero() { memset(data_, 0, sizeof(data_)); }

    void SetIndex(int32_t index) { index_ = index; }
    int32_t Index() const { return index_; }

 private:
    const char* End() const { return data_ + size; }

 private:
    char data_[size];
    int32_t index_;
    char* cur_;
};

} // namespace craft

#endif // __CRAFT_COMMON_FIXED_BUFFER_H__