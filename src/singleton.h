#ifndef __CRAFT_COMMON_SINGLETON_H__
#define __CRAFT_COMMON_SINGLETON_H__
#include <mutex>
#include <memory>

namespace craft {

// lazy singleton
template<typename T, bool is_thread_safe = true>
class Singleton {
 public:
    static T& Instance() {   
        if (instance_ == nullptr) {
            if (is_thread_safe) {
                std::unique_lock<std::mutex> unique_locker(mutex_);
                if (instance_ == nullptr) {
                    instance_ = std::unique_ptr<T>(new T);
                }
            } else {
                instance_ = std::unique_ptr<T>(new T);
            }
        }

        return *instance_;
    }

    Singleton(T&&) = delete;
    Singleton(const T&) = delete;
    void operator= (const T&) = delete;

 protected:
    Singleton() = default;
    virtual ~Singleton() = default;

 private:
    static std::unique_ptr<T> instance_;
    static std::mutex mutex_;
};

template<typename T, bool is_thread_safe>
std::unique_ptr<T> Singleton<T, is_thread_safe>::instance_;

template<typename T, bool is_thread_safe>
std::mutex Singleton<T, is_thread_safe>::mutex_;

} // namespace craft

#endif // __CRAFT_COMMON_SINGLETON_H__