#ifndef __CRAFT_NOCOPYABLE_H__
#define __CRAFT_NOCOPYABLE_H__

namespace craft {

// inheriting this class makes it impossible to copy.
class noncopyable {
 public:
    noncopyable(const noncopyable&) = delete;
    noncopyable(noncopyable&&) = delete;
    void operator=(const noncopyable&) = delete;

 protected:
    noncopyable() = default;
    ~noncopyable() = default;
};

} // namespace craft

#endif // __CRAFT_NOCOPYABLE_H__