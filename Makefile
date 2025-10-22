DEPS_PATH = _build
BUILD_DIR = $(DEPS_PATH)
__python_dir = $(DEPS_PATH)/python
__cache_dir = $(DEPS_PATH)/cache
__uv_dir = $(DEPS_PATH)/uv
uv_cache = $(__cache_dir)/uv
uv_config = $(__uv_dir)/bin/config
venv = $(__python_dir)/tmp/venv
py = $(venv)/bin/python3
UV = $(__uv_dir)/bin/uv

PYTHON_VERSION = 3.13 #change this if needed 

#remove build dir
.a.nuke: 
	rm -rf $(DEPS_PATH)
.PHONY: .a.nuke

.d.uv: $(uv_config)
.PHONY: .d.uv

$(uv_config): | $(uv_cache)
	rm -rf $(__python_dir)
	mkdir -p $(__python_dir)
	curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=$(__uv_dir)/bin/ UV_NO_MODIFY_PATH=1 sh
	echo "cache-dir = \"$(realpath $(uv_cache))\"" > $@
	touch $(UV)
	export UV_CONFIG_FILE=$@ && $(UV) python install $(PYTHON_VERSION) 
	
$(uv_cache):
	mkdir -p $@

$(venv)/.created: 
	$(UV) venv $(venv)
	touch $@
	touch $(py)

deps: |.d.uv $(venv)/.created
	$(eval VFLAG := $(if $(filter $(VERBOSE),1),--verbose,))
	export UV_PROJECT_ENVIRONMENT=$(venv) && $(UV) sync
.PHONY: deps

check: |deps clean
	$(py) -m flake8 src tests --count --select=E9,F63,F7,F82 --show-source --statistics; 
	$(py) -m flake8 src tests --count --max-complexity=10 --max-line-length=127 --statistics; 
.PHONY: check

clean: |deps
	$(py) -m isort src tests
	$(py) -m black src tests 
.PHONY: clean

test: | deps
	mkdir -p $(BUILD_DIR)/junit
	$(py) -m pytest  \
	  --cov src \
	  --cov-branch \
	  --cov-report term \
	  --cov-report xml:$(BUILD_DIR)/unit-cover.xml \
	  --cov-config .coveragerc \
	  -vv -rA -l \
	  --junit-xml=$(BUILD_DIR)/junit/unit.xml \
	  --timeout=200 \
	  tests/unit-tests
.PHONY: test
