package proxy

// ScopedError carries a stable error scope alongside a human-readable message.
//
//	err := proxy.NewScopedError("proxy.config", "load failed", io.EOF)
type ScopedError struct {
	Scope   string
	Message string
	Cause   error
}

type WrappedCause interface {
	Error() string
}

// NewScopedError creates an error that keeps a greppable scope token in the failure path.
//
//	err := proxy.NewScopedError("proxy.server", "listen failed", cause)
func NewScopedError(scope, message string, cause error) *ScopedError {
	return &ScopedError{
		Scope:   scope,
		Message: message,
		Cause:   cause,
	}
}

func (e *ScopedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause == nil {
		return e.Scope + ": " + e.Message
	}
	return e.Scope + ": " + e.Message + ": " + e.Cause.Error()
}

func (e *ScopedError) CauseError() WrappedCause {
	if e == nil {
		return nil
	}
	return e.Cause
}
