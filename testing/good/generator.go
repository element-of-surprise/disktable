package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
)

func main() {
	r, err := os.Open("./entries.csv")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	w, err := os.OpenFile("./new.csv", os.O_WRONLY+os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	br := bufio.NewReader(r)

	i := 1
	done := false
	for {
		if done {
			break
		}
		b, err := br.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			if len(b) == 0 {
				break
			}
		}
		end := len(b)
		if bytes.Equal(b[end-1:end], []byte("\n")) {
			end = len(b) - 1
		}
		b = append(b[:end], []byte(fmt.Sprintf(",%d\n", i))...)
		i++
		if _, err := w.Write(b); err != nil {
			panic(err)
		}
	}
}
