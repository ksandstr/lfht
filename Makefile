
CFLAGS=-std=gnu99 -O2 -Wall -g -I ~/src/ccan


all:
	@echo "nothing to do!"


clean:
	@rm -f *.o


distclean: clean
	@rm -rf .deps



%.o: %.c
	@echo "  CC $@"
	@$(CC) -c -o $@ $< $(CFLAGS) -MMD
	@test -d .deps || mkdir -p .deps
	@mv $(<:.c=.d) .deps/

.deps:
	@mkdir -p .deps
