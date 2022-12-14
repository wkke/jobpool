package command

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/mitchellh/cli"
	"github.com/posener/complete"
)


func mergeAutocompleteFlags(flags ...complete.Flags) complete.Flags {
	merged := make(map[string]complete.Predictor, len(flags))
	for _, f := range flags {
		for k, v := range f {
			merged[k] = v
		}
	}
	return merged
}

func commandErrorText(cmd NamedCommand) string {
	return fmt.Sprintf("For additional help try 'jobpool %s -help'", cmd.Name())
}

type uiErrorWriter struct {
	ui  cli.Ui
	buf bytes.Buffer
}


func (w *uiErrorWriter) Write(data []byte) (int, error) {
	read := 0
	for len(data) != 0 {
		a, token, err := bufio.ScanLines(data, false)
		if err != nil {
			return read, err
		}

		if a == 0 {
			r, err := w.buf.Write(data)
			return read + r, err
		}

		w.ui.Error(w.buf.String() + string(token))
		data = data[a:]
		w.buf.Reset()
		read += a
	}

	return read, nil
}

func (w *uiErrorWriter) Close() error {
	// emit what's remaining
	if w.buf.Len() != 0 {
		w.ui.Error(w.buf.String())
		w.buf.Reset()
	}
	return nil
}