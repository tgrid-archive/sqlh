package sqlh

import (
	"strconv"
)

// Update the index of argument placeholders.
// reindex("where a = $1 and b = $2", 5) -> "where a = $6 and b = $7"
func reindex(s string, base int) string {
	var (
		DEFAULT        = 1
		PARAMETER      = 2
		D_QUOTE        = 3
		D_QUOTE_ESCAPE = 4
		S_QUOTE        = 5
		S_QUOTE_ESCAPE = 6
	)
	state := DEFAULT
	var result []rune
	var capture []rune
	for _, c := range s {
		switch {
		case state == DEFAULT && c == '\'':
			state = S_QUOTE
			result = append(result, c)
		case state == DEFAULT && c == '"':
			state = D_QUOTE
			result = append(result, c)
		case state == DEFAULT && c == '$':
			state = PARAMETER
		case state == DEFAULT:
			result = append(result, c)
		case state == PARAMETER && '0' <= c && c <= '9':
			capture = append(capture, c)
		case state == PARAMETER:
			state = DEFAULT
			param := "$"
			if len(capture) > 0 {
				n, _ := strconv.Atoi(string(capture))
				param += strconv.Itoa(base + n)
			}
			result = append(result, []rune(param)...)
			result = append(result, c)
			capture = make([]rune, 0)
		case state == D_QUOTE && c == '\\':
			state = D_QUOTE_ESCAPE
			result = append(result, c)
		case state == D_QUOTE && c == '"':
			state = DEFAULT
			result = append(result, c)
		case state == D_QUOTE:
			result = append(result, c)
		case state == D_QUOTE_ESCAPE:
			state = D_QUOTE
			result = append(result, c)
		case state == S_QUOTE && c == '\\':
			state = S_QUOTE_ESCAPE
			result = append(result, c)
		case state == S_QUOTE && c == '\'':
			state = DEFAULT
			result = append(result, c)
		case state == S_QUOTE:
			result = append(result, c)
		case state == S_QUOTE_ESCAPE:
			state = S_QUOTE
			result = append(result, c)
		}
	}
	if state == PARAMETER {
		param := "$"
		if len(capture) > 0 {
			n, _ := strconv.Atoi(string(capture))
			param += strconv.Itoa(base + n)
		}
		result = append(result, []rune(param)...)
	}
	return string(result)
}
