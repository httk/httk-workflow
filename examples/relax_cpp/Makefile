# The C and C++ halves use their own language flags, then link together.
CXX := c++
CC := cc
C_SDK := c
CPP_SDK := cpp

relax: relax.cpp $(CPP_SDK)/httk_workflow.hpp $(C_SDK)/httk_workflow.c $(C_SDK)/httk_workflow.h
	$(CC) -std=c99 -Wall -Wextra -c $(C_SDK)/httk_workflow.c -I$(C_SDK) -o httk_workflow_c.o
	$(CXX) -std=c++17 -Wall -Wextra -I$(CPP_SDK) -o relax relax.cpp httk_workflow_c.o

clean:
	rm -f relax httk_workflow_c.o
