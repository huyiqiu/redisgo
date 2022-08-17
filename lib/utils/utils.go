package utils

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, c := range cmd {
		args[i] = []byte(c)
	}
	return args
}

// ToCmdLine2 convert commandName and []byte-type argument to CmdLine
func ToCmdLine2(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, c := range args {
		result[i+1] = c
	}
	return result
}

// BytesEquals checks whether the given bytes is equal
func BytesEquals(a []byte, b []byte) bool {
	if a == nil && b != nil || a != nil && b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i ++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}