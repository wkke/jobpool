package structs


// RpcError is used for serializing errors with a potential error code
type RpcError struct {
	Message string
	Code    *int64
}

func NewRpcError(err error, code *int64) *RpcError {
	return &RpcError{
		Message: err.Error(),
		Code:    code,
	}
}

func (r *RpcError) Error() string {
	return r.Message
}
