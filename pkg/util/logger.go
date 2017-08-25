package util

import (
	"errors"
	"io"
	"os"

	logging "github.com/op/go-logging"
)

// LogConfig - The configuration for the logging.
type LogConfig struct {
	LogFile string
	Stdout  bool
	Level   string
	Color   bool
}

var logConfig LogConfig
var logFile *os.File

// SetLogConfig - set the log configuration fo each module.
func SetLogConfig(lConfig LogConfig) error {
	logConfig = lConfig
	if logConfig.LogFile == "" && !logConfig.Stdout {
		return errors.New("Cannot have a blank logfile and not log to stdout")
	}
	var err error
	if _, err = os.Stat(logConfig.LogFile); os.IsNotExist(err) {
		if logFile, err = os.Create(logConfig.LogFile); err != nil {
			logFile.Close()
			return err
		}
	} else {
		if logFile, err = os.OpenFile(logConfig.LogFile, os.O_APPEND|os.O_WRONLY, 0666); err != nil {
			logFile.Close()
			return err
		}
	}
	return nil
}

// NewLog - Creates a new logging object for the module given
// TODO: Consider no output?
func NewLog(module string) *logging.Logger {
	// TODO: More validation? Check file is good?
	// TODO: Validate level is actually possible?

	var backends []logging.Backend

	logger := logging.MustGetLogger(module)

	colorFormatter := logging.MustStringFormatter(
		"%{color}[%{time}] [%{level}] [%{module}] %{message}%{color:reset}",
	)

	standardFormatter := logging.MustStringFormatter(
		"[%{time}] [%{level}] [%{module}] %{message}",
	)

	var formattedBackend = func(writer io.Writer, isColored bool) logging.Backend {
		backend := logging.NewLogBackend(writer, "", 0)
		formatter := standardFormatter
		if isColored {
			formatter = colorFormatter
		}
		return logging.NewBackendFormatter(backend, formatter)
	}

	if logConfig.LogFile != "" {
		backends = append(backends, formattedBackend(logFile, false))
	}

	if logConfig.Stdout {
		backends = append(backends, formattedBackend(os.Stdout, logConfig.Color))
	}

	if len(backends) == 0 {
		backends = append(backends, formattedBackend(os.Stdout, true))
	}

	multiBackend := logging.MultiLogger(backends...)
	logger.SetBackend(multiBackend)
	logging.SetLevel(levelFromString(logConfig.Level), module)

	return logger
}

func levelFromString(str string) logging.Level {
	var level logging.Level

	switch str {
	case "critical":
		level = logging.CRITICAL
	case "error":
		level = logging.ERROR
	case "warning":
		level = logging.WARNING
	case "notice":
		level = logging.NOTICE
	case "info":
		level = logging.INFO
	default:
		level = logging.DEBUG
	}

	return level
}
