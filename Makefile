################################################################################
# Variables                                                                    #
################################################################################
LOCAL_ARCH := $(shell uname -m)
ifeq ($(LOCAL_ARCH),x86_64)
	TARGET_ARCH_LOCAL=amd64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 5),armv8)
	TARGET_ARCH_LOCAL=arm64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 4),armv)
	TARGET_ARCH_LOCAL=arm
else
	TARGET_ARCH_LOCAL=amd64
endif
export GOARCH ?= $(TARGET_ARCH_LOCAL)

LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
   TARGET_OS_LOCAL = linux
else ifeq ($(LOCAL_OS),Darwin)
   TARGET_OS_LOCAL = darwin
else
   TARGET_OS_LOCAL ?= windows
endif
export GOOS ?= $(TARGET_OS_LOCAL)

ifeq ($(GOOS),windows)
BINARY_EXT_LOCAL:=.exe
GOLANGCI_LINT:=golangci-lint.exe
else
BINARY_EXT_LOCAL:=
GOLANGCI_LINT:=golangci-lint
endif

################################################################################
# Target: test                                                                 #
################################################################################
.PHONY: test
test:
	go test ./store/inmemory

################################################################################
# Target: table store integrationtest                                          #
######################ä#########################################################
check-tablestore-integration-test-args:
ifeq ($(storageaccount),)
	$(error arg strorageaccount must be set: strorageaccount="<name of account>")
endif
ifeq ($(storageaccountkey),)
	$(error arg storageaccountkey must be set: storageaccountkey="<key of account>")
endif

.PHONY: tablestore-integration-test
tablestore-integration-test: check-tablestore-integration-test-args
	go test ./store/azure/tablestorage -storageaccount="$(storageaccount)" -storageaccountkey="$(storageaccountkey)"

################################################################################
# Target: cosmosdb-integrationtest
################################################################################
check-cosmosdb-integration-test-args:
ifeq ($(url),)
	$(error arg url must be set)
endif
ifeq ($(masterKey),)
	$(error arg masterKey must be set)
endif
ifeq ($(database),)
	$(error arg database must be set)
endif
ifeq ($(container),)
	$(error arg container must be set)
endif

.PHONY: cosmosdb-integration-test
cosmosdb-integration-test: check-cosmosdb-integration-test-args
	go test ./store/azure/cosmosdb -url="$(url)" -masterKey="$(masterKey)" -database="$(database)" -container="$(container)"

################################################################################
# Target: lint                                                                 #
################################################################################
.PHONY: lint
lint:
	# Due to https://github.com/golangci/golangci-lint/issues/580, we need to add --fix for windows
	$(GOLANGCI_LINT) run --fix