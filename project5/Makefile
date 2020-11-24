.SUFFIXES: .cpp .o

CXX=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cpp
TARGET_OBJ:=$(SRCDIR)main.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:= \
	$(SRCDIR)bpt.cpp \
	$(SRCDIR)file.cpp \
	$(SRCDIR)buf.cpp \
	$(SRCDIR)lock_mgr.cpp \
	$(SRCDIR)trx.cpp

OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cpp=.o)

CXXFLAGS+= -g -std=c++17 -Wall -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET):
	$(foreach file, $(SRCS_FOR_LIB), $(CXX) $(CXXFLAGS) -o $(file:.cpp=.o) -c $(file) -lpthread;)
	$(CXX) $(CXXFLAGS) -o $(SRCDIR)main.o -c $(SRCDIR)main.cpp -lpthread	
	make static_library
	$(CXX) $(CXXFLAGS) -o $@ $(SRCDIR)main.o -L $(LIBS) -lbpt -lpthread

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	g++ -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
