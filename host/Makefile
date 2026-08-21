CXX ?= c++
# UBSan runs clean here; ASan's runtime hangs in dyld on this macOS toolchain
# (initializer deadlock before main), so it is opt-in via `make test-asan`.
CXXFLAGS ?= -std=c++17 -Wall -Wextra -g -fsanitize=undefined -fno-sanitize-recover=all
CORE = ../components/qnetd
SRCS = $(CORE)/msg.cpp $(CORE)/server.cpp

test: build/test
	./build/test

build/test: test_main.cpp $(SRCS) $(CORE)/*.h
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -I$(CORE) -o $@ test_main.cpp $(SRCS)

test-asan: test_main.cpp $(SRCS) $(CORE)/*.h
	@mkdir -p build
	$(CXX) -std=c++17 -Wall -Wextra -g -fsanitize=address,undefined -I$(CORE) \
	  -o build/test_asan test_main.cpp $(SRCS)
	./build/test_asan

build/qnetd_lite: qnetd_lite.cpp $(SRCS) $(CORE)/*.h
	@mkdir -p build
	$(CXX) -std=c++17 -Wall -O2 -I$(CORE) -o $@ qnetd_lite.cpp $(SRCS)

clean:
	rm -rf build

.PHONY: test test-asan clean
