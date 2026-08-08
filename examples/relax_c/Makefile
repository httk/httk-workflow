SDK := ../../src/httk/workflow/native/c
CFLAGS := -std=c99 -Wall -Wextra -I$(SDK)

relax: relax.c $(SDK)/httk_workflow.c $(SDK)/httk_workflow.h
	$(CC) $(CFLAGS) -o relax relax.c $(SDK)/httk_workflow.c

clean:
	rm -f relax
