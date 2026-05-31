.PHONY: clean validate run test db\:sync inject\:spk encrypt\:env decrypt\:env env\:encrypt env\:decrypt start

ZIG_BUILD := zig build -p .zig/build --cache-dir .zig/.cache
INJECT_SPK := DOTENV_PRIVATE_KEY="$$(pass @local/env)" dotenvx run
DOTENVX := $(INJECT_SPK) -f .env --

validate:
	@scripts/validate.sh

clean: validate
	rm -rf .zig-cache zig-out .zig

run: validate
	mkdir -p .zig/.cache .zig/build
	$(DOTENVX) $(ZIG_BUILD) run

test: validate
	mkdir -p .zig/.cache .zig/build
	$(DOTENVX) $(ZIG_BUILD) test

db\:sync: validate
	$(DOTENVX) scripts/db/sync.sh

inject\:spk: validate
	$(INJECT_SPK)

encrypt\:env env\:encrypt:
	$(INJECT_SPK) -- dotenvx encrypt

decrypt\:env env\:decrypt:
	$(INJECT_SPK) -- dotenvx decrypt

start: validate
	mkdir -p .zig/.cache .zig/build
	$(DOTENVX) $(ZIG_BUILD) start
