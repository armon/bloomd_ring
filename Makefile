APP = bloomd

# The environment we're building for. This mainly affects what
# overlay variables are used for rel creation.
ENVIRONMENT ?= development

all: deps compile

compile:
	./rebar compile

clean:
	./rebar clean

deps:
	./rebar get-deps

devrel: rel
	rm -rf rel/$(APP)/lib/$(APP)-*/ebin
	ln -sf $(abspath ./ebin) rel/$(APP)/lib/$(APP)-*

rel: compile
	-ps aux | grep epmd | grep -v grep | awk '{ print $$2 }' | xargs kill
	./rebar generate -f overlay_vars=vars/$(ENVIRONMENT).config

relcluster: compile
	-ps aux | grep epmd | grep -v grep | awk '{ print $$2 }' | xargs kill
	./rebar generate -f target_dir=node1 overlay_vars=vars/node1.config
	./rebar generate -f target_dir=node2 overlay_vars=vars/node2.config
	./rebar generate -f target_dir=node3 overlay_vars=vars/node3.config

updatecluster:
	rm -rf rel/node1/lib/$(APP)-*/ebin
	cp -r $(abspath ./ebin) rel/node1/lib/$(APP)-*
	rm -rf rel/node2/lib/$(APP)-*/ebin
	cp -r $(abspath ./ebin) rel/node2/lib/$(APP)-*
	rm -rf rel/node3/lib/$(APP)-*/ebin
	cp -r $(abspath ./ebin) rel/node3/lib/$(APP)-*

startcluster:
	rel/node3/bin/iris start
	rel/node2/bin/iris start
	rel/node1/bin/iris start

stopcluster:
	rel/node3/bin/iris stop &
	rel/node2/bin/iris stop &
	rel/node1/bin/iris stop &

test: compile
	./rebar eunit apps=$(APP)

.PHONY: all compile clean deps
