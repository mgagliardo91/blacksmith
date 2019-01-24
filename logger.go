package blacksmith

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/teris-io/shortid"
)

// LogFormatFn overloads the logging log.fmt(string, format)
type LogFormatFn func(string, ...interface{})

// LogFn overloads the standard log.xx()
type LogFn func(...interface{})

// LogProvider represents an entity that can log. All logged statements will be formatted
// with the identity of the LogProvider
type LogProvider struct {
	id     string
	name   string
	prefix string
}

var sid, _ = shortid.New(1, shortid.DEFAULT_ABC, 2342)

// InitLog initializes the LogProvider
func (lP *LogProvider) InitLog(name string) *LogProvider {
	lP.name = name
	lP.id, _ = sid.Generate()

	return lP
}

// SetPrefix passes a parent identity that will be logged at the start of each log statement
func (lP *LogProvider) SetPrefix(prefix string) *LogProvider {
	lP.prefix = prefix
	return lP
}

// Identifier returns a A-ID|B-ID|C-ID pattern for the LogProvider
func (lP LogProvider) Identifier() string {
	identifier := lP._identifier()

	if lP.prefix != "" {
		identifier = fmt.Sprintf("%s>%s", lP.prefix, identifier)
	}

	return identifier
}

func (lP LogProvider) _identifier() string {
	return fmt.Sprintf("%s-%s", lP.name, lP.id)
}

func (lP LogProvider) buildLogPrefix() string {
	logStatement := fmt.Sprintf("[%s]:", lP._identifier())

	if lP.prefix != "" {
		logStatement = fmt.Sprintf("(%s) %s", lP.prefix, logStatement)
	}

	return logStatement
}

// Logf prints the value and arguments using the standard log.Printf
func (lP LogProvider) Logf(value string, args ...interface{}) {
	lP.LogfUsing(log.Printf, value, args...)
}

// LogfUsing prints the value and arguments using the provided LogFormatFn
func (lP LogProvider) LogfUsing(logFn LogFormatFn, value string, args ...interface{}) {
	logStatement := fmt.Sprintf("%s %s", lP.buildLogPrefix(), value)
	logFn(logStatement, args)
}

// Log prints the value and arguments using the standard log.Println
func (lP LogProvider) Log(value string) {
	lP.LogUsing(log.Println, value)
}

// LogUsing prints the value and arguments using the provided LogFn
func (lP LogProvider) LogUsing(logFn LogFn, value string, args ...interface{}) {
	logStatement := fmt.Sprintf("%s %s", lP.buildLogPrefix(), value)

	if len(args) > 0 {
		logFn(logStatement, args)
	} else {
		logFn(logStatement)
	}
}
