package testutils

import (
	"bufio"
	"fmt"
	"io"
)

// GenerateCSVFixture writes a fast, sequential CSV stream with realistic columns.
// Schema: id (int64), customer_id (string), amount (float64), status (string), created_at (string).
func GenerateCSVFixture(w io.Writer, numRecords int64) error {
	bw := bufio.NewWriterSize(w, 64*1024)
	defer bw.Flush()

	if _, err := bw.WriteString("id,customer_id,amount,status,created_at\n"); err != nil {
		return err
	}

	statuses := []string{"COMPLETED", "PENDING", "PROCESSING", "FAILED"}

	for i := int64(1); i <= numRecords; i++ {
		custID := fmt.Sprintf("CUST-%06d", i%100000)
		amount := 100.50 + float64(i%500)
		status := statuses[i%int64(len(statuses))]
		createdAt := "2026-08-18T10:00:00Z"

		line := fmt.Sprintf("%d,%s,%.2f,%s,%s\n", i, custID, amount, status, createdAt)
		if _, err := bw.WriteString(line); err != nil {
			return err
		}
	}

	return bw.Flush()
}
