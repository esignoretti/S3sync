package main

import "github.com/esignoretti/S3sync/cmd"

// version is the build version, injected at build time with
// -ldflags "-X main.version=<version>". It stays "dev" for source builds.
var version = "dev"

func main() {
	cmd.Execute(version)
}
