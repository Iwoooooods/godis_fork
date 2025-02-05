package utils

func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, str := range cmd {
		args[i] = []byte(str)
	}
	return args
}
