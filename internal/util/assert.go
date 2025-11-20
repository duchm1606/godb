package util

// Assert panics with the given message if condition is false.
// This is intended for internal invariants and should not be used
// for user input validation.
func Assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
