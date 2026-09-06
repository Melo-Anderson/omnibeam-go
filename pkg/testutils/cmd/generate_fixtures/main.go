package main

import (
	"log"

	"github.com/omnibeam/dataflow-compute-go/pkg/testutils"
)

func main() {
	if err := testutils.GenerateCleanSampleCSV("testdata/fixtures/clean_sample.csv", 5000); err != nil {
		log.Fatalf("failed generating clean sample: %v", err)
	}
	if err := testutils.GenerateLatin1SemicolonGz("testdata/fixtures/latin1_semicolon.csv.gz", 2000); err != nil {
		log.Fatalf("failed generating latin1 sample: %v", err)
	}
	if err := testutils.GenerateMalformedSampleCSV("testdata/fixtures/malformed_sample.csv", 1000); err != nil {
		log.Fatalf("failed generating malformed sample: %v", err)
	}
	log.Println("Successfully generated all test fixtures in testdata/fixtures/")
}
