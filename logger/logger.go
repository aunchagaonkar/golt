package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

var Logger zerolog.Logger

func Init(nodeID string) {

	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	Logger = zerolog.New(output).With().
		Timestamp().
		Str("node", nodeID).
		Logger()
}

func InitJSON(nodeID string) {
	Logger = zerolog.New(os.Stdout).With().
		Timestamp().
		Str("node", nodeID).
		Logger()
}

func WithRequestID(requestID string) zerolog.Logger {
	return Logger.With().Str("request_id", requestID).Logger()
}

func Info() *zerolog.Event {
	return Logger.Info()
}

func Debug() *zerolog.Event {
	return Logger.Debug()
}

func Warn() *zerolog.Event {
	return Logger.Warn()
}

func Error() *zerolog.Event {
	return Logger.Error()
}
