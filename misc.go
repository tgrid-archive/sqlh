package sqlh

func repeat(s, sep string, n int) string {
	if n < 0 {
		panic("n < 0")
	}
	v := ""
	ssep := ""
	for i := 0; i < n; i++ {
		v += ssep + s
		ssep = sep
	}
	return v
}
