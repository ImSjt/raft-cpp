#include <iostream>
#include <memory>

class Test
{
public:
    Test() {
        std::cout << "begin" << std::endl;
    }

    ~Test() {
        std::cout << "end" << std::endl;
    }
};

int main(int argc, char* argv[])
{
    std::unique_ptr<Test> p(new Test);

    p = nullptr;

    p.reset(new Test);
}