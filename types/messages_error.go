package types

import (
	"errors"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

var ErrUnexpectedOffset = errors.New("unexpected offset")

type MessageError struct {
	Message *kafka.Message
	Err     error
}

func (m MessageError) Error() string {
	return fmt.Sprintf("partition %d: offset %d: %s", m.Message.Partition, m.Message.Offset, m.Err.Error())
}

// Unwrap helps errors.Is() to detect context error
func (m MessageError) Unwrap() error {
	return m.Err
}

type MessageErrors []MessageError

func (m MessageErrors) Error() string {
	sb := strings.Builder{}
	sb.WriteString("messages errors: ")
	for i, e := range m {
		if i != 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(e.Error())
	}
	return sb.String()
}
