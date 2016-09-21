package main

type Error int

const (
	ErrNoNeedfullFields Error = 0x01
)

// Error returns the error as a string.
func (e Error) Error() string {
	switch e {
	case ErrNoNeedfullFields:
		return "No fields which necessary for a table."
	default:
		return "Unknown error"
	}
}
