package dbfly

import (
	"errors"
	"fmt"
	"strings"
)

type Error []error

func (e Error) Error() string {
	logWithNumber := make([]string, lenWithoutNil(e))
	for i, l := range e {
		if l != nil {
			logWithNumber[i] = fmt.Sprintf(" - %d: %s", i+1, l.Error())
		}
	}

	return fmt.Sprintf("Aggregate Error List:\n%s", strings.Join(logWithNumber, "\n"))
}

func lenWithoutNil(e Error) (count int) {
	for _, v := range e {
		if v != nil {
			count++
		}
	}

	return
}

func Join(errs ...error) error {
	return Error(errs)
}

type messageWrappedError struct {
	err error
	msg string
}

func (e *messageWrappedError) Error() string {
	return fmt.Sprintf("%s\n - Error: %v", e.msg, e.err)
}

func (e *messageWrappedError) Unwrap() error {
	return e.err
}

func New(msg string, args ...any) error {
	if len(args) == 0 {
		return errors.New(msg)
	}
	return fmt.Errorf(msg, args...)
}

func Wrap(err error, msg string, args ...any) error {
	if err == nil {
		return New(msg, args...)
	}
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	return &messageWrappedError{
		err: err,
		msg: msg,
	}
}
