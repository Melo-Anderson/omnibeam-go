package testutils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/text/encoding/charmap"
)

func GenerateCleanSampleCSV(destPath string, count int) error {
	_ = os.MkdirAll(filepath.Dir(destPath), 0755)
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, _ = f.WriteString("id,customer_id,amount,created_at,is_active\n")
	now := time.Now().UTC().Format(time.RFC3339)
	for i := 1; i <= count; i++ {
		_, _ = fmt.Fprintf(f, "%d,cust_%04d,%.2f,%s,true\n", i, i, float64(i)*10.5, now)
	}
	return nil
}

func GenerateLatin1SemicolonGz(destPath string, count int) error {
	_ = os.MkdirAll(filepath.Dir(destPath), 0755)
	var buf bytes.Buffer
	buf.WriteString("id;cidade;valor;data_cadastro\n")
	for i := 1; i <= count; i++ {
		buf.WriteString(fmt.Sprintf("%d;São Paulo;%.2f;2026-08-14 12:00:00\n", i, float64(i)*5.25))
	}

	encoder := charmap.ISO8859_1.NewEncoder()
	latin1Bytes, err := encoder.Bytes(buf.Bytes())
	if err != nil {
		return err
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	_, err = gw.Write(latin1Bytes)
	if closeErr := gw.Close(); err == nil {
		err = closeErr
	}
	return err
}

func GenerateMalformedSampleCSV(destPath string, validCount int) error {
	_ = os.MkdirAll(filepath.Dir(destPath), 0755)
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, _ = f.WriteString("id,customer_id,amount,created_at,is_active\n")
	now := time.Now().UTC().Format(time.RFC3339)
	for i := 1; i <= validCount; i++ {
		_, _ = fmt.Fprintf(f, "%d,cust_%04d,%.2f,%s,true\n", i, i, float64(i)*10.5, now)
	}

	// 25 invalid dates
	for i := 1; i <= 25; i++ {
		_, _ = fmt.Fprintf(f, "%d,cust_bad_date,10.00,invalid-date-format,true\n", 10000+i)
	}
	// 25 string in int64
	for i := 1; i <= 25; i++ {
		_, _ = fmt.Fprintf(f, "not_an_int,cust_bad_int,10.00,%s,true\n", now)
	}
	// 10 nulls in non-nullable column
	for i := 1; i <= 10; i++ {
		_, _ = fmt.Fprintf(f, ",cust_null_id,10.00,%s,true\n", now)
	}
	return nil
}
