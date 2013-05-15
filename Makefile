REBAR = ./rebar

# ERLC_OPTS = +debug_info

all: $(EBIN_DIR)
	$(REBAR) get-deps
	$(REBAR) compile
#	cp converter_app.app.src ebin/converter_app.app








clean:
	$(REBAR) clean

ctags:
	cd $(SRC_DIR) ; ctags -R . ../include 

$(EBIN_DIR) :
	( test -d $(EBIN_DIR) || mkdir -p $(EBIN_DIR) )

tests: 
	$(REBAR) skip_deps=true eunit  
