# The two SDK halves are compiled with their own compilers: the Fortran standard
# flag is not valid for C, and vice versa, so the C source is compiled to an
# object with $(CC) and linked into the Fortran build with $(FC).
FC := gfortran
C_SDK := ../../src/httk/workflow/native/c
F_SDK := ../../src/httk/workflow/native/fortran
FFLAGS := -std=f2008 -Wall -Wextra

relax: relax.f90 $(F_SDK)/httk_workflow.f90 $(C_SDK)/httk_workflow.c $(C_SDK)/httk_workflow.h
	$(CC) -std=c99 -c $(C_SDK)/httk_workflow.c -o httk_workflow_c.o
	$(FC) $(FFLAGS) -o relax $(F_SDK)/httk_workflow.f90 relax.f90 httk_workflow_c.o

clean:
	rm -f relax httk_workflow_c.o httk_workflow.mod relax_steps.mod
