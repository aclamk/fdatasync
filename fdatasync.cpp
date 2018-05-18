#include <mutex>
#include <thread>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <malloc.h>
#include <iostream>
#include <sys/time.h>
#include <assert.h>


long long int now()
{
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec*1000000+tv.tv_usec;
}


char* data;
std::string file_name;

static constexpr size_t data_size = 256*1024*1024;
static constexpr size_t data_align = 4*1024;
static constexpr int num_chunks = 3;


int do_test(bool is_direct, bool parallel_fdatasync)
{
  std::mutex section_lock[num_chunks];
  std::thread worker[num_chunks];
  int fds[num_chunks];

  section_lock[1].lock();
  section_lock[2].lock();

  auto worker_action = [&section_lock, &fds, parallel_fdatasync](int index) {
    for(int i=0; i<100; i++) {
      section_lock[index].lock();
      for(int j=0; j<10; j++) {
	size_t size = 4096 << (rand()%5);
	size_t pos = (rand() % ((data_size - size)/data_align)) * data_align;
	assert( pwrite(fds[index], &data[pos], size, pos) == size );
      }
      if (parallel_fdatasync) 
	section_lock[(index+1) % num_chunks].unlock();
      fdatasync(fds[index]);
      if (!parallel_fdatasync)
	section_lock[(index+1) % num_chunks].unlock();
    }
  };

  std::cout << "Test: flags=" << (is_direct?"O_DIRECT":"0") << " parallel=" << parallel_fdatasync << std::endl;
  long long start = now();
    
  for (int i = 0; i < num_chunks; i++) {
    fds[i] = open(file_name.c_str(), O_RDWR | O_CREAT | (is_direct?O_DIRECT:0), 0666);
    assert (fds[i] >= 0);
    worker[i] = std::thread{worker_action, i};
  }
  

  for (int i = 0; i < num_chunks; i++) {
    worker[i].join();
    close(fds[i]);
  }
  std::cout << "time =  " << now() - start << " usec" << std::endl;
    
  return 0;
}


int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage " << argv[0] << " test_file(block_dev)" << std::endl;
    return -1;
  }

  data = (char*) memalign(data_align, data_size);
  file_name = argv[1];
  do_test(true, true);
  do_test(true, false);
  do_test(false, true);
  do_test(false, false);
  
  return 0;
}
