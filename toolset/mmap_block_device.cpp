#include <unistd.h>
#include <atomic>
#include <algorithm>
#include <cstdlib>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <docopt.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <signal.h>
#include <random>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

static const char USAGE[] =
    R"(usage: mmap_block_device [options] DEVICE_PATH

options:
  -h --help

)";

static docopt::Options d;

int main (int argc, char **argv, char **envp)
{
    dieunless (argc == 2);
    string devpath (argv[1]);
    int fd = open (devpath.c_str (), O_RDONLY);
    dieunless (fd > -1);

    struct stat sbuf;
    dieunless (fstat (fd, &sbuf) != -1);

    dieunless ((sbuf.st_mode & S_IFMT) == S_IFBLK);
    uint64_t devsize = 0;

    dieunless (ioctl (fd, BLKGETSIZE64, &devsize) != -1);
    printf ("Opened block device %s:\n\tPreferred I/O block size:\t%jd bytes\n\tDevice size:\t%jd bytes\n", devpath.c_str (), sbuf.st_blksize, devsize);

    dieunless (devsize > (1024*1024));

    void *base = mmap (NULL, devsize, PROT_READ, MAP_SHARED | MAP_NORESERVE, fd, 0);

    dieunless (base != (void *)-1);
    
    for (int ii=0; ii<100; ii++) {
	uint64_t boffset = 1152 + ii;
	uint8_t bval = ((uint8_t*)base)[boffset];
	printf ("offset %lu has value %d\n", boffset, bval);
    }

    munmap (base, devsize);

    close (fd);

    return 0;

}
