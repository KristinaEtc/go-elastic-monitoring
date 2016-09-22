package main

type Error int

const (
	ErrNoNeedfullFields Error = 0x01
	ErrUnknownUTCFormat Error = 0x02
)

// Error returns the error as a string.
func (e Error) Error() string {
	switch e {
	case ErrNoNeedfullFields:
		return "No fields which necessary for a table."
	case ErrUnknownUTCFormat:
		return "Unknown UTC Format"
	default:
		return "Unknown error"
	}
}
