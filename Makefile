all: fdatasync

fdatasync: fdatasync.cpp
	g++ -o $@ $^ -lpthread
