#include <iostream>
#include <memory>
#include <vector>

struct T {
    int a;
};

using TPtr = std::shared_ptr<T>;

int test(const TPtr& p) {
    p->a = 2;
    return 0;
}

int main(int argc, char* argv[]) {
    auto p = std::make_shared<T>();
    test(p);
    p.get();
    std::cout << p->a << std::endl;
    return 0;
}