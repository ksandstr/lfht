
CCAN_DIR=~/src/ccan

CFLAGS:=-O2 -std=gnu11 -Wall -g -march=native \
	-D_GNU_SOURCE -pthread -I $(CCAN_DIR) -I $(abspath .) \
	-DCCAN_LIST_DEBUG=1 #-DDEBUG_ME_HARDER

TEST_BIN:=$(patsubst t/%.c,t/%,$(wildcard t/*.c))
MAIN_OBJS:=$(patsubst %.c,%.o,$(wildcard *.c))


all: tags $(TEST_BIN)


clean:
	@rm -f *.o t/*.o $(TEST_BIN)


distclean: clean
	@rm -f tags
	@rm -rf .deps


check: $(TEST_BIN)
	prove -v -m $(sort $(TEST_BIN))


tags: $(shell find . -iname "*.[ch]" -or -iname "*.p[lm]")
	@ctags -R *


t/%: t/%.o $(MAIN_OBJS) \
		ccan-list.o ccan-htable.o ccan-hash.o ccan-tap.o \
		ccan-talloc.o
	@echo "  LD $@"
	@$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(LIBS)


ccan-%.o ::
	@echo "  CC $@ <ccan>"
	@$(CC) -c -o $@ $(CCAN_DIR)/ccan/$*/$*.c $(CFLAGS)


%.o: %.c
	@echo "  CC $@"
	@$(CC) -c -o $@ $< $(CFLAGS) -MMD
	@test -d .deps || mkdir -p .deps
	@mv $(<:.c=.d) .deps/


include $(wildcard .deps/*.d)
