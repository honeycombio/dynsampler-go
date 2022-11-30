.PHONY: test
#: run the tests!
test:
ifeq (, $(shell which gotestsum))
	@echo " ***"
	@echo "Running with standard go test because gotestsum was not found on PATH. Consider installing gotestsum for friendlier test output!"
	@echo " ***"
	go test -race -v ./...
else
	gotestsum --junitfile unit-tests.xml --format testname -- -race ./...
endif

#########################
###     RELEASES      ###
#########################

CIRCLE_TAG ?=
RELEASE_VERSION ?= $(or $(CIRCLE_TAG), $(shell git describe --tags))

.PHONY: publish_github
#: draft a GitHub release for current commit/tag and upload builds as its assets
publish_github: github_prereqs
	@echo "+++ drafting GitHub release, tag $(RELEASE_VERSION)"
	@ghr -draft \
	     -name ${RELEASE_VERSION} \
	     -token ${GITHUB_TOKEN} \
	     -username ${CIRCLE_PROJECT_USERNAME} \
	     -repository ${CIRCLE_PROJECT_REPONAME} \
	     -commitish ${CIRCLE_SHA1} \
	     ${RELEASE_VERSION}

.PHONY: github_prereqs
github_prereqs: ghr_present
	@:$(call check_defined, RELEASE_VERSION, the tag from which to create this release)
	@:$(call check_defined, GITHUB_TOKEN, auth to create this release)
	@:$(call check_defined, CIRCLE_PROJECT_USERNAME, user who will create this release)
	@:$(call check_defined, CIRCLE_PROJECT_REPONAME, the repository getting a new release)
	@:$(call check_defined, CIRCLE_SHA1, the git ref to associate with this release)


#################
### Utilities ###
#################

.PHONY: ghr_present
ghr_present:
	@which ghr || (echo "ghr missing; required to create release at GitHub"; exit 1)

check_defined = \
    $(strip $(foreach 1,$1, \
        $(call __check_defined,$1,$(strip $(value 2)))))
__check_defined = \
    $(if $(value $1),, \
        $(error Undefined $1$(if $2, ($2))$(if $(value @), \
                required by target `$@')))
