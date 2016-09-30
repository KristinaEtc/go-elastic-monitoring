package main

import "errors"

// Error values
var (
	ErrNoNeedfullFields = errors.New("No fields which necessary for a table.")
	ErrUnknownUTCFormat = errors.New("Unknown UTC Format")
)
