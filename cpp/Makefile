OS_NAME=`uname -s`
SRC_DIR=src
INC_DIR=$(SRC_DIR)/include

RM=rm
ECHO=echo
MKDIR=mkdir

EXE=bin/sqliteloader
#SRCS=$(SRC_DIR)/main.cpp
SRCS=$(SRC_DIR)/main.cpp
SRCS+=$(SRC_DIR)/OptionParser.cpp
SRCS+=$(SRC_DIR)/cJSON.c
OBJS=$(SRCS:.c=.o)
#OBJS+=$(SRCS:.cpp=.o)

INCS=$($(INC_DIR)/*.h)

CC_INC_DIR=-I$(INC_DIR)
CC_LIB_DIR=-Llib/$(OS_NAME)
CC_LIB_DIR=-Llib/Linux
CC_LIBS=-lsqlite3

OBJECT_MODE=64
CC=g++
CFLAGS=-g -m$(OBJECT_MODE) -I$(CC_INC_DIR)
LDFLAGS=-g -m$(OBJECT_MODE) $(CC_LIB_DIR)


all: $(EXE)


$(EXE): $(OBJS) Makefile
	-$(MKDIR) -p bin
	$(CC) $(LDFLAGS) $(OBJS) -o $@ $(CC_LIBS)

.c.o: $(INCS) $(SRCS) Makefile
	-$(ECHO) $(OBJS)
	$(CC) $(CFLAGS) -c $< -o $@ 

clean:
	-$(RM) -f $(SRC_DIR)/*.o
	-$(RM) -rf bin/*

tags: $(SRCS) $(INCS)
	cscope -CbqR
	ctags --c++-kinds=+cdefglmnpstuvx -R

test:
