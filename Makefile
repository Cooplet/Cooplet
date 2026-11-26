include .devkit/runtime/devkit.mk

ifeq ($(strip $(value app)$(value package)),)
# Devkit check discovers Rust tests. Run the workspace policy tests as well.
check: workspace-fitness
endif

.PHONY: workspace-fitness
workspace-fitness:
	$(MAKE) test app=node
