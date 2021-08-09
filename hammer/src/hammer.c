#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>


int main() {
  srandom(clock());

  const size_t total_size = 512*100*1000; // TODO: get from region.json

  while (1) {
    size_t offset = random() % total_size;
    size_t sz = random() % (4096);

    while (((offset + sz) > total_size) || (sz == 0)) {
      sz = random() % (4096*10);
    }

    printf("read write test for %lu %lu\n", offset, sz);

    uint8_t * buf = malloc(sz);
    uint8_t * buf2 = malloc(sz);
    FILE * fp;
    int fn;
    size_t r;

    // get some random data
    fp = fopen("/dev/random", "rb");
    fn = fileno(fp);

    if (sz != read(fn, buf, sz)) {
      free(buf);
      free(buf2);
      return 1;
    }
    fclose(fp);

    // write out random data
    fp = fopen("/dev/nbd0", "wb");
    fn = fileno(fp);

    if (0 != fseek(fp, offset, SEEK_SET)) {
      free(buf);
      free(buf2);
      return 2;
    }

    if (sz != (r = write(fn, buf, sz))) {
      printf("write fail at %lu %lu\n", offset, sz);
      free(buf);
      free(buf2);
      return 3;
    }

    if (0 != fsync(fn)) {
      free(buf);
      free(buf2);
      return 4;
    }

    fclose(fp);

    // Read data
    fp = fopen("/dev/nbd0", "rb");
    fn = fileno(fp);

    if (0 != fseek(fp, offset, SEEK_SET)) {
      free(buf);
      free(buf2);
      return 5;
    }

    if (sz != (r = read(fn, buf2, sz))) {
      printf("read fail at %lu %lu: %lu\n", offset, sz, r);
      free(buf);
      free(buf2);
      return 6;
    }

    fclose(fp);

    if (memcmp(buf, buf2, sz) != 0) {
      printf("memcmp fail at %lu %lu\n", offset, sz);
      abort();

      for (size_t i = 0; i < sz; i++) {
        if (buf[i] != buf2[i]) {
          printf("%lu offset is bad: 0x%X != 0x%X\n", i, buf[i], buf2[i]);
        } else {
          printf("%lu offset is  ok: 0x%X == 0x%X\n", i, buf[i], buf2[i]);
        }
      }
      free(buf);
      free(buf2);
      return 7;
    }

    free(buf);
    free(buf2);
  }

  return 0;
}
