CC := cc
ADA := gnatmake
C_SDK := ../../src/httk/workflow/native/c
ADA_SDK := ../../src/httk/workflow/native/ada
ADAFLAGS := -gnat2012 -gnatwa -gnatwe

relax: relax.adb relax_steps.adb relax_steps.ads $(ADA_SDK)/httk_workflow.adb $(ADA_SDK)/httk_workflow.ads $(C_SDK)/httk_workflow.c $(C_SDK)/httk_workflow.h
	$(CC) -std=c99 -c $(C_SDK)/httk_workflow.c -o httk_workflow_c.o
	$(ADA) $(ADAFLAGS) -I$(ADA_SDK) -o relax relax.adb -largs httk_workflow_c.o

clean:
	# The wildcards include relax_steps and all other Ada/GNAT intermediates.
	rm -f relax *.o *.ali b~* gnatbind.* gnatlink.*
