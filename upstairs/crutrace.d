provider crutrace {
    probe gw_read_start(uint64_t);
    probe gw_write_start(uint64_t);
    probe gw_flush_start(uint64_t);
    probe gw_read_end(uint64_t);
    probe gw_write_end(uint64_t);
    probe gw_flush_end(uint64_t);
};
