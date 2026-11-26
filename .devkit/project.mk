# Managed component registration.
DEVKIT_REGISTERED_APPS := history hub node telegram
DEVKIT_REGISTERED_PACKAGES := cooplet-client cooplet-platform cooplet-protocol cooplet-storage workflow-sdk
DEVKIT_DESKTOP_APPS := 
DEVKIT_PACKAGEABLE_PACKAGES := workflow-sdk
setup-history: setup-rust-workspace
check-history: check/packages/cooplet-client build/packages/cooplet-client check/packages/cooplet-platform build/packages/cooplet-platform check/packages/cooplet-protocol build/packages/cooplet-protocol
test-history: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
build-history: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
dev-history: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
setup-hub: setup-rust-workspace
check-hub: check/packages/cooplet-client build/packages/cooplet-client check/packages/cooplet-platform build/packages/cooplet-platform check/packages/cooplet-protocol build/packages/cooplet-protocol check/packages/cooplet-storage build/packages/cooplet-storage
test-hub: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
build-hub: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
dev-hub: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
setup-node: setup-rust-workspace
check-node: check/packages/cooplet-client build/packages/cooplet-client check/packages/cooplet-platform build/packages/cooplet-platform check/packages/cooplet-protocol build/packages/cooplet-protocol check/packages/cooplet-storage build/packages/cooplet-storage
test-node: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
build-node: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
dev-node: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol build/packages/cooplet-storage
setup-telegram: setup-rust-workspace
check-telegram: check/packages/cooplet-client build/packages/cooplet-client check/packages/cooplet-platform build/packages/cooplet-platform check/packages/cooplet-protocol build/packages/cooplet-protocol
test-telegram: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
build-telegram: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
dev-telegram: build/packages/cooplet-client build/packages/cooplet-platform build/packages/cooplet-protocol
setup/packages/cooplet-client: setup-rust-workspace
check/packages/cooplet-client: check/packages/cooplet-protocol build/packages/cooplet-protocol
test/packages/cooplet-client: build/packages/cooplet-protocol
build/packages/cooplet-client: build/packages/cooplet-protocol
setup/packages/cooplet-platform: setup-rust-workspace
setup/packages/cooplet-protocol: setup-rust-workspace
setup/packages/cooplet-storage: setup-rust-workspace
check/packages/cooplet-storage: check/packages/cooplet-platform build/packages/cooplet-platform check/packages/cooplet-protocol build/packages/cooplet-protocol
test/packages/cooplet-storage: build/packages/cooplet-platform build/packages/cooplet-protocol
build/packages/cooplet-storage: build/packages/cooplet-platform build/packages/cooplet-protocol
setup/packages/workflow-sdk: setup-node-workspace
package/packages/workflow-sdk: build/packages/workflow-sdk
DEVKIT_EXPECTED_PLAN := cf8df1764be87ca08b5210a4f5e3a9fef47188cfca43c04e5ff9c5d5d5e0bbc1
