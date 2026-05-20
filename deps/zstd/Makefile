STD=
WARN= -Wall
OPT= -O3

R_CFLAGS= $(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS)
R_LDFLAGS= $(LDFLAGS)
DEBUG= -g

R_CC=$(CC) $(R_CFLAGS)
R_LD=$(CC) $(R_LDFLAGS)

AR ?= ar
ARFLAGS= rcs

libzstd.a: zstd.o
	$(AR) $(ARFLAGS) $@ $^

zstd.o: zstd.c zstd.h zstd_errors.h

.c.o:
	$(R_CC) -fPIC -c $<

.PHONY: clean
clean:
	rm -f *.o
	rm -f *.a
