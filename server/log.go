package server

import (
	"sync/atomic"
)


type Logger interface {
	Noticef(format string, v ...interface{})

	Warnf(format string, v ...interface{})

	Fatalf(format string, v ...interface{})

	Errorf(format string, v ...interface{})

	Debugf(format string, v ...interface{})

	Tracef(format string, v ...interface{})
}

func (s *RPCServer) Logger() Logger{
	s.logging.Lock()
	defer s.logging.Unlock()
	return s.logging.logger
}

// Noticef logs a notice statement
func (s *RPCServer) Noticef(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Noticef(format, v...)
	}, format, v...)
}


// Errorf logs an error
func (s *RPCServer) Errorf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Errorf(format, v...)
	}, format, v...)
}

// Warnf logs a warning error
func (s *RPCServer) Warnf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Warnf(format, v...)
	}, format, v...)
}

// Fatalf logs a fatal error
func (s *RPCServer) Fatalf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func (s *RPCServer) Debugf(format string, v ...interface{}) {
	if atomic.LoadInt32(&s.logging.debug) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Debugf(format, v...)
	}, format, v...)
}

// Tracef logs a trace statement
func (s *RPCServer) Tracef(format string, v ...interface{}) {
	if atomic.LoadInt32(&s.logging.trace) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Tracef(format, v...)
	}, format, v...)
}

func (s *RPCServer) executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}){
	s.logging.RLock()
	defer s.logging.RUnlock()
	if s.logging.logger == nil {
		return
	}
	f(s.logging.logger, format, args...)
}