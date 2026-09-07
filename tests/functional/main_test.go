package functional_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// repoRoot is the absolute path to the repository root, shared across all functional tests.
var (
	repoRoot        string
	testPipelineBin string
)

func TestMain(m *testing.M) {
	var err error
	repoRoot, err = filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed determining repo root: %v\n", err)
		os.Exit(1)
	}

	tmpDir, err := os.MkdirTemp("", "omnibeam-test-bin-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed creating temp dir for test binary: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	binName := "pipeline"
	if runtime.GOOS == "windows" {
		binName = "pipeline.exe"
	}
	testPipelineBin = filepath.Join(tmpDir, binName)

	buildCmd := exec.Command("go", "build", "-o", testPipelineBin, filepath.Join(repoRoot, "cmd", "pipeline"))
	buildCmd.Env = append(os.Environ(), "GOTMPDIR="+filepath.Join(repoRoot, ".gotmp"))
	if out, err := buildCmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "failed building pipeline binary for test suite: %v\nOutput:\n%s\n", err, string(out))
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}
