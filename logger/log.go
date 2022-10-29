package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

type Logger struct {
	sync.Mutex
	logger     *log.Logger
	debug      bool
	trace      bool
	infoLabel  string
	warnLabel  string
	errorLabel string
	fatalLabel string
	debugLabel string
	traceLabel string
	fl         *fileLogger
}

func NewStdLogger(time, debug, trace, colors, pid bool) *Logger {
	flags := 0
	if time {
		flags = log.LstdFlags | log.Lmicroseconds
	}

	pre := ""
	if pid {
		pre = pidPrefix()
	}

	l := &Logger{
		logger: log.New(os.Stderr, pre, flags),
		debug:  debug,
		trace:  trace,
	}

	if colors {
		setColoredLabelFormats(l)
	} else {
		setPlainLabelFormats(l)
	}

	return l
}

func NewFileLogger(filename string, time, debug, trace, pid bool) *Logger {
	flags := 0
	if time {
		flags = log.LstdFlags | log.Lmicroseconds
	}

	pre := ""
	if pid {
		pre = pidPrefix()
	}

	fl, err := newFileLogger(filename, pre, time)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		return nil
	}

	l := &Logger{
		logger: log.New(fl, pre, flags),
		debug:  debug,
		trace:  trace,
		fl:     fl,
	}
	fl.Lock()
	fl.l = l
	fl.Unlock()

	setPlainLabelFormats(l)
	return l
}

type writerAndCloser interface {
	Write(b []byte) (int, error)
	Close() error
	Name() string
}

type fileLogger struct {
	out       int64
	canRotate int32
	sync.Mutex
	l      *Logger
	f      writerAndCloser
	limit  int64
	olimit int64
	pid    string
	time   bool
	closed bool
}

func newFileLogger(filename, pidPrefix string, time bool) (*fileLogger, error) {
	fileflags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	f, err := os.OpenFile(filename, fileflags, 0600)
	if err != nil {
		return nil, err
	}
	stats, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fl := &fileLogger{
		canRotate: 0,
		f:         f,
		out:       stats.Size(),
		pid:       pidPrefix,
		time:      time,
	}
	return fl, nil
}

// 获取pid的string
func pidPrefix() string {
	return fmt.Sprintf("[%d] ", os.Getpid())
}

func setColoredLabelFormats(l *Logger) {
	colorFormat := "[\x1b[%sm%s\x1b[0m] "
	l.infoLabel = fmt.Sprintf(colorFormat, "32", "INF")
	l.debugLabel = fmt.Sprintf(colorFormat, "36", "DBG")
	l.warnLabel = fmt.Sprintf(colorFormat, "0;93", "WRN")
	l.errorLabel = fmt.Sprintf(colorFormat, "31", "ERR")
	l.fatalLabel = fmt.Sprintf(colorFormat, "31", "FTL")
	l.traceLabel = fmt.Sprintf(colorFormat, "33", "TRC")
}

func setPlainLabelFormats(l *Logger) {
	l.infoLabel = "[INF] "
	l.debugLabel = "[DBG] "
	l.warnLabel = "[WRN] "
	l.errorLabel = "[ERR] "
	l.fatalLabel = "[FTL] "
	l.traceLabel = "[TRC] "
}

// SetSizeLimit sets the size of a logfile after a backup
// is created with the file name + "year.month.day.hour.min.sec.naosec"
// and the current the log is truncated
func (l *Logger) SetSizeLimit(limit int64) error {
	l.Lock()
	if l.fl == nil {
		l.Unlock()
		return fmt.Errorf("can set log size limit only for file logger")
	}
	fl := l. fl
	l.Unlock()
	fl.setLimit(limit)
	return nil
}

func (l *fileLogger) setLimit(limit int64) {
	l.Lock()
	l.olimit, l.limit = limit, limit
	atomic.StoreInt32(&l.canRotate, 1)
	rotateNow := l.out > l.limit
	l.Unlock()
	if rotateNow {
		l.l.Noticef("Rotating logfile")
	}
}

func (l *fileLogger) Write(b []byte) (int, error) {
	if atomic.LoadInt32(&l.canRotate) == 0 {
		n, err := l.f.Write(b)
		if err == nil {
			atomic.AddInt64(&l.out, int64(n))
		}
		return n, err
	}
	l.Lock()
	n, err := l.f.Write(b)
	// if err == nil {
	// 	l.out += int64(n)
	// 	if l.out > l.limit {
	// 		if err := l.f.Close(); err != nil {
	// 			l.limit *= 2
	// 			l.logDirect(l.l.errorLabel, "Unable to close logfile for rotation (%v), will attempt next rotation at size %v", err, l.limit)
	// 			l.Unlock()
	// 			return n, err
	// 		}
	// 		fname := l.f.Name()
	// 		now := time.Now()
	// 		bak := fmt.Sprintf("%s.%04d.%02d.%02d.%02d.%02d.%02d.%09d", fname,
	// 			now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(),
	// 			now.Second(), now.Nanosecond())
	// 		os.Rename(fname, bak)
	// 		fileflags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	// 		f, err := os.OpenFile(fname, fileflags, 0660)
	// 		if err != nil {
	// 			l.Unlock()
	// 			panic(fmt.Sprintf("Unable to re-open the logfile %q after rotation: %v", fname, err))
	// 		}
	// 		l.f = f
	// 		n := l.logDirect(l.l.infoLabel, "Rotated log, backup saved as %q", bak)
	// 		l.out = int64(n)
	// 		l.limit = l.olimit
	// 	}
	// }
	l.Unlock()
	return n, err
}

// Noticef logs a notice statement
func (l *Logger) Noticef(format string, v ...interface{}) {
	l.logger.Printf(l.infoLabel+format, v...)
}

// Warnf logs a notice statement
func (l *Logger) Warnf(format string, v ...interface{}) {
	l.logger.Printf(l.warnLabel+format, v...)
}

// Errorf logs an error statement
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logger.Printf(l.errorLabel+format, v...)
}

// Fatalf logs a fatal error
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logger.Fatalf(l.fatalLabel+format, v...)
}

// Debugf logs a debug statement
func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.logger.Printf(l.debugLabel+format, v...)
	}
}

// Tracef logs a trace statement
func (l *Logger) Tracef(format string, v ...interface{}) {
	if l.trace {
		l.logger.Printf(l.traceLabel+format, v...)
	}
}
