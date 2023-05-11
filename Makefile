
SRC_ROOT = .
BIN_DIR = $(SRC_ROOT)/bin
GOCMD ?= go
GOBUILD = $(GOCMD) build -v
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test -v -failfast

ifndef $(GOPATH)
GOPATH=$(shell echo $(shell $(GOCMD) env GOPATH) | sed -E "s;(.*):.*;\1;")
export GOPATH
endif

all: build

build: bin
	@echo "+ building from source"
	$(GOBUILD) -o $(BIN_DIR)

bin:
	@mkdir $@

test:
	@echo "+ executing tests"
	$(GOCLEAN) -testcache && $(GOTEST) $(SRC_ROOT)/...