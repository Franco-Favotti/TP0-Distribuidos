package util

import "errors"

//Simula un split
func SplitFields(line string, delimiter byte) []string {
	var fields []string
	start := 0
	for i := 0; i < len(line); i++ {
		if line[i] == delimiter {
			fields = append(fields, line[start:i])
			start = i + 1
		}
	}
	fields = append(fields, line[start:])
	return fields
}

//Convierte un string a un entero
func ParseInt(s string) (int, error) {
	if len(s) == 0 {
		return 0, errors.New("cadena vacía, no se puede convertir a número")
	}
	negative := false
	i := 0
	if s[0] == '-' {
		negative = true
		i = 1
	}

	if i == len(s) {
		return 0, errors.New("cadena inválida: " + s)
	}
	result := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, errors.New("carácter no numérico en: " + s)
		}
		result = result*10 + int(c-'0')
	}
	if negative {
		result = -result
	}
	return result, nil
}

//Convierte un entero a un string
func ParseString(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	if negative {
		return "-" + string(digits)
	}
	return string(digits)
}