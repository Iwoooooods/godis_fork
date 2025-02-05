package utils

import (
	"testing"
)

func TestConvertTOCmdLine(t *testing.T) {
	var output string

	cmds := []string{"set", "name", "hehuaisen"}
	args := ToCmdLine(cmds...)
	for _, b := range args {
		output += string(b)
		output += " "
	}
	t.Logf("%v", output)
}
